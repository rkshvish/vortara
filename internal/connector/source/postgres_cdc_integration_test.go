//go:build integration

package source

import (
	"context"
	"fmt"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/rkshvish/vortara/pkg/config"
	"github.com/rkshvish/vortara/pkg/row"
)

func startLogicalPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Cmd:          []string{"-c", "wal_level=logical"},
		Env: map[string]string{
			"POSTGRES_USER":     "vortara",
			"POSTGRES_PASSWORD": "vortara",
			"POSTGRES_DB":       "vortara",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(90 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("skipping integration test; unable to start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")
	return fmt.Sprintf("postgres://vortara:vortara@%s:%s/vortara?sslmode=disable", host, port.Port())
}

func collectCDCEvents(t *testing.T, out <-chan row.Row, n int, timeout time.Duration) []row.Row {
	t.Helper()
	var events []row.Row
	deadline := time.After(timeout)
	for len(events) < n {
		select {
		case r := <-out:
			events = append(events, r)
		case <-deadline:
			t.Fatalf("timeout: collected %d/%d CDC events", len(events), n)
		}
	}
	return events
}

// TestPostgresCDC_Integration covers the full log-based capture loop:
// insert/update/delete decoding, ack-based LSN confirmation, and resume
// from the confirmed position after a reconnect.
func TestPostgresCDC_Integration(t *testing.T) {
	dsn := startLogicalPostgres(t)

	pgExec(t, dsn,
		`CREATE TABLE deals (id BIGSERIAL PRIMARY KEY, name TEXT, amount INT)`,
	)

	cfg := config.StreamingConfig{
		Type:     "postgres_cdc",
		Endpoint: dsn,
		Options:  map[string]string{"table": "deals"},
	}

	src := NewPostgresCDCSource()
	if err := src.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	out := make(chan row.Row, 100)
	subCtx, cancel := context.WithCancel(context.Background())
	subDone := make(chan error, 1)
	go func() { subDone <- src.Subscribe(subCtx, out) }()

	// Give the replication stream a moment to start before writing.
	time.Sleep(1 * time.Second)

	pgExec(t, dsn,
		`INSERT INTO deals (name, amount) VALUES ('alpha', 100), ('beta', 200)`,
		`UPDATE deals SET amount = 150 WHERE name = 'alpha'`,
		`DELETE FROM deals WHERE name = 'beta'`,
	)

	events := collectCDCEvents(t, out, 4, 20*time.Second)

	// Two inserts, one update, one delete — in commit order.
	if op := events[0].Data["_op"]; op != "insert" || events[0].Data["name"] != "alpha" {
		t.Fatalf("event 0 = %v", events[0].Data)
	}
	if op := events[1].Data["_op"]; op != "insert" || events[1].Data["name"] != "beta" {
		t.Fatalf("event 1 = %v", events[1].Data)
	}
	if op := events[2].Data["_op"]; op != "update" || events[2].Data["amount"] != "150" {
		t.Fatalf("event 2 = %v", events[2].Data)
	}
	if op := events[3].Data["_op"]; op != "delete" {
		t.Fatalf("event 3 = %v", events[3].Data)
	}
	if events[0].PrimaryKey != "id=1" {
		t.Fatalf("PrimaryKey = %q, want id=1 from replica identity", events[0].PrimaryKey)
	}
	if events[0].Watermark.IsZero() {
		t.Fatal("Watermark should carry the commit timestamp")
	}

	// Ack everything, let a standby status update flush the LSN.
	for _, e := range events {
		if err := src.Ack(context.Background(), e.ID); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	time.Sleep(6 * time.Second) // > standby update interval

	// Stop and reconnect: acked changes must NOT redeliver; new ones must.
	cancel()
	<-subDone
	_ = src.Close()

	src2 := NewPostgresCDCSource()
	if err := src2.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer src2.Close()
	out2 := make(chan row.Row, 100)
	sub2Ctx, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	sub2Done := make(chan error, 1)
	go func() { sub2Done <- src2.Subscribe(sub2Ctx, out2) }()
	time.Sleep(1 * time.Second)

	pgExec(t, dsn, `INSERT INTO deals (name, amount) VALUES ('gamma', 300)`)

	resumed := collectCDCEvents(t, out2, 1, 20*time.Second)
	if resumed[0].Data["name"] != "gamma" || resumed[0].Data["_op"] != "insert" {
		t.Fatalf("resumed event = %v, want only the new gamma insert", resumed[0].Data)
	}
	// No stale redeliveries queued behind it.
	select {
	case extra := <-out2:
		t.Fatalf("unexpected redelivery after resume: %v", extra.Data)
	case <-time.After(2 * time.Second):
	}
}
