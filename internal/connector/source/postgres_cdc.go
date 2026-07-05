package source

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/rkshvish/vortara/internal/registry"
	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

// PostgresCDCSource streams row changes from PostgreSQL logical replication
// (pgoutput plugin) — log-based change data capture instead of polling.
//
// Delivery contract: each emitted row carries the WAL position of its commit.
// Ack advances the confirmed LSN (the engine acks in order), which is flushed
// to the server on the periodic standby status update; the replication slot
// then never re-sends acknowledged changes, even across restarts. Unacked
// changes are redelivered on reconnect — at-least-once, deduplicated
// downstream by the delivery log for merge strategies.
type PostgresCDCSource struct {
	dsn         string
	table       string
	slot        string
	publication string

	conn *pgconn.PgConn

	mu        sync.Mutex
	rowLSN    map[string]pglogrepl.LSN // rowID → commit LSN
	confirmed pglogrepl.LSN
}

var _ StreamingSource = (*PostgresCDCSource)(nil)

func init() {
	registry.RegisterStreamingSource("postgres_cdc", func() any {
		return NewPostgresCDCSource()
	})
}

// NewPostgresCDCSource returns a new PostgresCDCSource.
func NewPostgresCDCSource() *PostgresCDCSource {
	return &PostgresCDCSource{rowLSN: make(map[string]pglogrepl.LSN)}
}

// sanitizeIdent keeps lowercase alphanumerics and underscores for use in
// generated slot/publication names.
func sanitizeIdent(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Connect opens a replication connection and ensures the publication and
// replication slot exist.
func (p *PostgresCDCSource) Connect(ctx context.Context, cfg config.StreamingConfig) error {
	dsn := strings.TrimSpace(cfg.Endpoint)
	if dsn == "" {
		return errors.New("postgres_cdc source: url is required")
	}
	table := strings.TrimSpace(cfg.Options["table"])
	if table == "" {
		return errors.New("postgres_cdc source: table is required")
	}
	slot := strings.TrimSpace(cfg.Options["slot"])
	if slot == "" {
		slot = "vortara_" + sanitizeIdent(table)
	}
	publication := strings.TrimSpace(cfg.Options["publication"])
	if publication == "" {
		publication = "vortara_pub_" + sanitizeIdent(table)
	}

	// Logical replication requires replication=database on the connection.
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	conn, err := pgconn.Connect(ctx, dsn+sep+"replication=database")
	if err != nil {
		return fmt.Errorf("postgres_cdc source: connect: %w", err)
	}

	// Publication (idempotent).
	quotedTable := quoteTableName(table)
	createPub := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", quoteIdentifier(publication), quotedTable)
	if _, err := conn.Exec(ctx, createPub).ReadAll(); err != nil {
		if !isDuplicateObjectErr(err) {
			_ = conn.Close(ctx)
			return fmt.Errorf("postgres_cdc source: create publication: %w", err)
		}
	}

	// Permanent replication slot (idempotent).
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil && !isDuplicateObjectErr(err) {
		_ = conn.Close(ctx)
		return fmt.Errorf("postgres_cdc source: create slot: %w", err)
	}

	p.dsn = dsn
	p.table = table
	p.slot = slot
	p.publication = publication
	p.conn = conn
	return nil
}

func isDuplicateObjectErr(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "42710" || pgErr.Code == "42P07" // duplicate_object / duplicate_table
	}
	return false
}

// confirmedFlushLSN reads the slot's server-side confirmed position.
func (p *PostgresCDCSource) confirmedFlushLSN(ctx context.Context) (pglogrepl.LSN, error) {
	query := fmt.Sprintf(
		"SELECT COALESCE(confirmed_flush_lsn::text, '0/0') FROM pg_replication_slots WHERE slot_name = '%s'",
		strings.ReplaceAll(p.slot, "'", "''"),
	)
	results, err := p.conn.Exec(ctx, query).ReadAll()
	if err != nil {
		return 0, err
	}
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return 0, fmt.Errorf("replication slot %q not found", p.slot)
	}
	return pglogrepl.ParseLSN(string(results[0].Rows[0][0]))
}

// Subscribe streams decoded changes until ctx is cancelled or a fatal error.
func (p *PostgresCDCSource) Subscribe(ctx context.Context, out chan<- row.Row) error {
	if p.conn == nil {
		return errors.New("postgres_cdc source: not connected")
	}

	startLSN, err := p.confirmedFlushLSN(ctx)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.confirmed = startLSN
	p.mu.Unlock()

	err = pglogrepl.StartReplication(ctx, p.conn, p.slot, startLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", p.publication),
		}})
	if err != nil {
		return fmt.Errorf("postgres_cdc source: start replication: %w", err)
	}

	relations := map[uint32]*pglogrepl.RelationMessage{}
	var commitTime time.Time
	var txBuffer []row.Row // current transaction, emitted on commit
	standbyDeadline := time.Now().Add(5 * time.Second)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(standbyDeadline) {
			p.mu.Lock()
			flush := p.confirmed
			p.mu.Unlock()
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, p.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: flush, WALFlushPosition: flush, WALApplyPosition: flush}); err != nil {
				return fmt.Errorf("postgres_cdc source: status update: %w", err)
			}
			standbyDeadline = time.Now().Add(5 * time.Second)
		}

		recvCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		rawMsg, err := p.conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // deadline: send standby update next loop
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("postgres_cdc source: receive: %w", err)
		}

		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return err
			}
			if pkm.ReplyRequested {
				standbyDeadline = time.Time{} // reply on next loop iteration
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return err
			}
			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return err
			}
			switch msg := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				relations[msg.RelationID] = msg
			case *pglogrepl.BeginMessage:
				commitTime = msg.CommitTime
				txBuffer = txBuffer[:0]
			case *pglogrepl.InsertMessage:
				if r, ok := p.decode(relations[msg.RelationID], msg.Tuple, "insert", commitTime); ok {
					txBuffer = append(txBuffer, r)
				}
			case *pglogrepl.UpdateMessage:
				if r, ok := p.decode(relations[msg.RelationID], msg.NewTuple, "update", commitTime); ok {
					txBuffer = append(txBuffer, r)
				}
			case *pglogrepl.DeleteMessage:
				if r, ok := p.decode(relations[msg.RelationID], msg.OldTuple, "delete", commitTime); ok {
					txBuffer = append(txBuffer, r)
				}
			case *pglogrepl.CommitMessage:
				// Stamp rows with the position AFTER the transaction so an
				// ack-confirmed LSN never re-sends this transaction.
				endLSN := msg.TransactionEndLSN
				for _, r := range txBuffer {
					p.mu.Lock()
					p.rowLSN[r.ID] = endLSN
					p.mu.Unlock()
					select {
					case out <- r:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				txBuffer = txBuffer[:0]
			}
		}
	}
}

// decode converts a tuple into a Row; emission happens at commit time.
func (p *PostgresCDCSource) decode(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData, op string, commitTime time.Time) (row.Row, bool) {
	if rel == nil || tuple == nil {
		return row.Row{}, false
	}
	data := make(map[string]interface{}, len(rel.Columns)+1)
	var pkParts []string
	for i, col := range rel.Columns {
		if i >= len(tuple.Columns) {
			break
		}
		tcol := tuple.Columns[i]
		var val interface{}
		switch tcol.DataType {
		case 'n': // null
			val = nil
		case 'u': // unchanged TOAST value — not included in the WAL record
			val = nil
		default: // 't' text
			val = string(tcol.Data)
		}
		data[col.Name] = val
		if col.Flags == 1 && val != nil { // part of the replica identity key
			pkParts = append(pkParts, fmt.Sprintf("%s=%v", col.Name, val))
		}
	}
	data["_op"] = op

	r := row.Row{
		ID:          uuid.NewString(),
		Source:      "postgres_cdc." + p.table,
		Data:        data,
		ExtractedAt: time.Now(),
		Watermark:   commitTime,
	}
	if len(pkParts) > 0 {
		r.PrimaryKey = strings.Join(pkParts, ",")
	} else {
		r.PrimaryKey = r.ID
	}
	return r, true
}

// Ack advances the confirmed LSN to the acknowledged row's commit position.
// The engine acks rows in emission order, so this is monotonic.
func (p *PostgresCDCSource) Ack(ctx context.Context, rowID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if lsn, ok := p.rowLSN[rowID]; ok {
		if lsn > p.confirmed {
			p.confirmed = lsn
		}
		delete(p.rowLSN, rowID)
	}
	return nil
}

// Nack leaves the confirmed LSN untouched; the change redelivers on restart.
func (p *PostgresCDCSource) Nack(ctx context.Context, rowID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.rowLSN, rowID)
	return nil
}

// Close closes the replication connection. The slot is permanent so the
// stream resumes from the confirmed LSN on the next start.
func (p *PostgresCDCSource) Close() error {
	if p.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := p.conn.Close(ctx)
		p.conn = nil
		return err
	}
	return nil
}
