package destination

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/rkshvish/vortaraos/internal/state"
	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

func newPostgresDestinationTestConfig() config.DestinationConfig {
	return config.DestinationConfig{
		Connection: "postgres://example",
		MatchOn:    "id",
		Options:    map[string]string{"table": "deals"},
	}
}

func newSQLMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func upsertQueryPattern() string {
	return `INSERT INTO "public"\."deals" \("id",\s*"name"\) VALUES \(\$1,\s*\$2\) ON CONFLICT \("id"\) DO UPDATE SET "name" = EXCLUDED\."name"`
}

type fakeCopySession struct {
	mu        sync.Mutex
	copyCalls int
	execCalls int
	copyErr   error
	execErr   error
	loaded    int64
}

func (f *fakeCopySession) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execCalls++
	return pgconn.CommandTag{}, f.execErr
}

func (f *fakeCopySession) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, source pgx.CopyFromSource) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.copyCalls++
	if f.copyErr != nil {
		return 0, f.copyErr
	}
	var loaded int64
	for source.Next() {
		if _, err := source.Values(); err != nil {
			return loaded, err
		}
		loaded++
	}
	if err := source.Err(); err != nil {
		return loaded, err
	}
	f.loaded = loaded
	return loaded, nil
}

func (f *fakeCopySession) Release() {}

func withStrategy(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, "vortara_strategy", name)
}

func withRunID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, "vortara_run_id", id)
}

func TestPostgresDestination_Connect_MissingTable(t *testing.T) {
	dst := NewPostgresDestination()
	cfg := newPostgresDestinationTestConfig()
	cfg.Options["table"] = ""

	if err := dst.Connect(context.Background(), cfg); err == nil {
		t.Fatal("expected error")
	}
}

func TestPostgresDestination_Connect_MissingMatchOn(t *testing.T) {
	dst := NewPostgresDestination()
	cfg := newPostgresDestinationTestConfig()
	cfg.MatchOn = ""

	if err := dst.Connect(context.Background(), cfg); err == nil {
		t.Fatal("expected error")
	}
}

func TestPostgresDestination_Load_AlreadyDelivered(t *testing.T) {
	db, mock := newSQLMockDB(t)
	dst := NewPostgresDestination()
	dst.db = db
	dst.cfg = newPostgresDestinationTestConfig()

	store := state.NewMemoryStore()
	rw := row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": 1, "name": "foo"}, time.Now())
	if err := store.MarkDelivered(rw.ID, "pipeline", "dest"); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}

	res, err := dst.Load(context.Background(), []row.Row{rw}, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Skipped != 1 {
		t.Fatalf("expected 1 skipped, got %d", res.Skipped)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db calls: %v", err)
	}
}

func TestPostgresDestination_Load_Success(t *testing.T) {
	db, mock := newSQLMockDB(t)
	dst := NewPostgresDestination()
	dst.db = db
	dst.cfg = newPostgresDestinationTestConfig()

	store := state.NewMemoryStore()
	rw := row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": 1, "name": "foo"}, time.Now())

	mock.ExpectExec(upsertQueryPattern()).WithArgs(1, "foo").WillReturnResult(sqlmock.NewResult(1, 1))

	res, err := dst.Load(context.Background(), []row.Row{rw}, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 1 {
		t.Fatalf("expected 1 loaded, got %d, errors=%v", res.Loaded, res.Errors)
	}
	if len(res.Errors) != 0 {
		t.Fatalf("expected no errors, got %d", len(res.Errors))
	}
	delivered, err := store.IsDelivered(rw.ID, "pipeline", "dest")
	if err != nil {
		t.Fatalf("IsDelivered: %v", err)
	}
	if !delivered {
		t.Fatal("expected row to be marked delivered")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db calls: %v", err)
	}
}

func TestPostgresDestination_Load_DBError(t *testing.T) {
	db, mock := newSQLMockDB(t)
	dst := NewPostgresDestination()
	dst.db = db
	dst.cfg = newPostgresDestinationTestConfig()

	store := state.NewMemoryStore()
	rw := row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": 1, "name": "foo"}, time.Now())

	mock.ExpectExec(upsertQueryPattern()).WithArgs(1, "foo").WillReturnError(sql.ErrConnDone)

	res, err := dst.Load(context.Background(), []row.Row{rw}, store, "pipeline", "dest")
	if err == nil {
		t.Fatal("expected top-level error")
	}
	if res.Loaded != 0 {
		t.Fatalf("expected 0 loaded, got %d", res.Loaded)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db calls: %v", err)
	}
}

func TestPostgresDestination_Load_MultipleRows(t *testing.T) {
	db, mock := newSQLMockDB(t)
	dst := NewPostgresDestination()
	dst.db = db
	dst.cfg = newPostgresDestinationTestConfig()

	store := state.NewMemoryStore()
	rows := []row.Row{
		row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": 1, "name": "foo"}, time.Now()),
		row.NewRow("source", "pipeline", "id=2", map[string]interface{}{"id": 2, "name": "bar"}, time.Now()),
		row.NewRow("source", "pipeline", "id=3", map[string]interface{}{"id": 3, "name": "baz"}, time.Now()),
	}

	mock.ExpectExec(upsertQueryPattern()).WithArgs(1, "foo").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(upsertQueryPattern()).WithArgs(2, "bar").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(upsertQueryPattern()).WithArgs(3, "baz").WillReturnResult(sqlmock.NewResult(1, 1))

	res, err := dst.Load(context.Background(), rows, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 3 {
		t.Fatalf("expected 3 loaded, got %d", res.Loaded)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db calls: %v", err)
	}
}

func TestPostgresDestination_UsesInsertForMerge(t *testing.T) {
	db, mock := newSQLMockDB(t)
	dst := NewPostgresDestination()
	dst.db = db
	dst.cfg = newPostgresDestinationTestConfig()
	dst.cfg.Strategy = "merge"

	store := state.NewMemoryStore()
	rw := row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": 1, "name": "foo"}, time.Now())

	mock.ExpectExec(upsertQueryPattern()).WithArgs(1, "foo").WillReturnResult(sqlmock.NewResult(1, 1))

	res, err := dst.Load(withStrategy(context.Background(), "merge"), []row.Row{rw}, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 1 {
		t.Fatalf("expected 1 loaded, got %d", res.Loaded)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db calls: %v", err)
	}
}

func TestPostgresDestination_UsesCopyForAppend(t *testing.T) {
	db, mock := newSQLMockDB(t)
	dst := NewPostgresDestination()
	dst.db = db
	dst.cfg = newPostgresDestinationTestConfig()
	dst.cfg.Strategy = "append"
	fake := &fakeCopySession{}
	dst.acquireSession = func(context.Context) (copySession, error) { return fake, nil }

	store := state.NewMemoryStore()
	rows := []row.Row{
		row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": 1, "name": "foo"}, time.Now()),
		row.NewRow("source", "pipeline", "id=2", map[string]interface{}{"id": 2, "name": "bar"}, time.Now()),
	}
	for i := 0; i < 100; i++ {
		rows = append(rows, row.NewRow("source", "pipeline", "id=3", map[string]interface{}{"id": 3, "name": "baz"}, time.Now()))
	}

	res, err := dst.Load(withStrategy(context.Background(), "append"), rows, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != len(rows) {
		t.Fatalf("expected %d loaded, got %d", len(rows), res.Loaded)
	}
	if fake.copyCalls != 1 {
		t.Fatalf("expected copy load, got %d calls", fake.copyCalls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db calls: %v", err)
	}
}

func TestPostgresDestination_UsesCopyForReplace(t *testing.T) {
	db, mock := newSQLMockDB(t)
	dst := NewPostgresDestination()
	dst.db = db
	dst.cfg = newPostgresDestinationTestConfig()
	dst.cfg.Strategy = "replace"
	fake := &fakeCopySession{}
	dst.acquireSession = func(context.Context) (copySession, error) { return fake, nil }

	store := state.NewMemoryStore()
	rows := make([]row.Row, 100, 100)
	for i := 0; i < 100; i++ {
		rows[i] = row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": i + 1, "name": "foo"}, time.Now())
	}

	// Engine-managed replace runs write to a per-run staging table via COPY;
	// the target is only touched at FinalizeRun (atomic swap).
	mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE IF NOT EXISTS "public"."deals_vstg_1" (LIKE "public"."deals" INCLUDING DEFAULTS)`)).WillReturnResult(sqlmock.NewResult(0, 0))
	res, err := dst.Load(withStrategy(withRunID(context.Background(), 1), "replace"), rows, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != len(rows) {
		t.Fatalf("expected %d loaded, got %d", len(rows), res.Loaded)
	}
	if fake.copyCalls != 1 {
		t.Fatalf("expected copy load into staging, got %d calls", fake.copyCalls)
	}

	// Successful finalize: one transaction truncates the target and swaps
	// the staging rows in, then the staging table is dropped.
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`TRUNCATE TABLE "public"."deals"`)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO "public"."deals" SELECT * FROM "public"."deals_vstg_1"`)).WillReturnResult(sqlmock.NewResult(0, 100))
	mock.ExpectCommit()
	mock.ExpectExec(regexp.QuoteMeta(`DROP TABLE IF EXISTS "public"."deals_vstg_1"`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := dst.FinalizeRun(context.Background(), 1, true); err != nil {
		t.Fatalf("FinalizeRun() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db calls: %v", err)
	}
}

func TestPostgresDestination_ReplaceFailedRunDropsStaging(t *testing.T) {
	db, mock := newSQLMockDB(t)
	dst := NewPostgresDestination()
	dst.db = db
	dst.cfg = newPostgresDestinationTestConfig()
	dst.cfg.Strategy = "replace"
	fake := &fakeCopySession{}
	dst.acquireSession = func(context.Context) (copySession, error) { return fake, nil }

	store := state.NewMemoryStore()
	rows := make([]row.Row, 100, 100)
	for i := 0; i < 100; i++ {
		rows[i] = row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": i + 1, "name": "foo"}, time.Now())
	}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE IF NOT EXISTS "public"."deals_vstg_7" (LIKE "public"."deals" INCLUDING DEFAULTS)`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if _, err := dst.Load(withStrategy(withRunID(context.Background(), 7), "replace"), rows, store, "pipeline", "dest"); err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Failed run: the target is never touched — staging is just dropped.
	mock.ExpectExec(regexp.QuoteMeta(`DROP TABLE IF EXISTS "public"."deals_vstg_7"`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := dst.FinalizeRun(context.Background(), 7, false); err != nil {
		t.Fatalf("FinalizeRun(failed) error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db calls: %v", err)
	}
}

func TestPostgresDestination_SmallBatchUsesInsert(t *testing.T) {
	db, mock := newSQLMockDB(t)
	dst := NewPostgresDestination()
	dst.db = db
	dst.cfg = newPostgresDestinationTestConfig()
	dst.cfg.Strategy = "append"
	fake := &fakeCopySession{}
	dst.acquireSession = func(context.Context) (copySession, error) { return fake, nil }

	store := state.NewMemoryStore()
	rw := row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": 1, "name": "foo"}, time.Now())

	mock.ExpectExec(upsertQueryPattern()).WithArgs(1, "foo").WillReturnResult(sqlmock.NewResult(1, 1))

	res, err := dst.Load(withStrategy(context.Background(), "append"), []row.Row{rw}, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 1 {
		t.Fatalf("expected 1 loaded, got %d", res.Loaded)
	}
	if fake.copyCalls != 0 {
		t.Fatalf("expected insert path, got copy calls %d", fake.copyCalls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db calls: %v", err)
	}
}

func TestPostgresDestination_CopyFallback(t *testing.T) {
	db, mock := newSQLMockDB(t)
	dst := NewPostgresDestination()
	dst.db = db
	dst.cfg = newPostgresDestinationTestConfig()
	dst.cfg.Strategy = "append"
	dst.cfg.Options["copy_threshold"] = "1"
	fake := &fakeCopySession{copyErr: errors.New("copy failed")}
	dst.acquireSession = func(context.Context) (copySession, error) { return fake, nil }

	store := state.NewMemoryStore()
	rows := []row.Row{
		row.NewRow("source", "pipeline", "id=1", map[string]interface{}{"id": 1, "name": "foo"}, time.Now()),
		row.NewRow("source", "pipeline", "id=2", map[string]interface{}{"id": 2, "name": "bar"}, time.Now()),
	}

	mock.ExpectExec(upsertQueryPattern()).WithArgs(1, "foo").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(upsertQueryPattern()).WithArgs(2, "bar").WillReturnResult(sqlmock.NewResult(1, 1))

	res, err := dst.Load(withStrategy(context.Background(), "append"), rows, store, "pipeline", "dest")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if res.Loaded != 2 {
		t.Fatalf("expected 2 loaded, got %d", res.Loaded)
	}
	if fake.copyCalls != 1 {
		t.Fatalf("expected 1 copy attempt, got %d", fake.copyCalls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db calls: %v", err)
	}
}

func TestQuoteIdentifier(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "deals", want: `"deals"`},
		{in: "MyTable", want: `"MyTable"`},
		{in: "my table", want: `"my table"`},
		{in: `say "hello"`, want: `"say ""hello"""`},
		{in: "select", want: `"select"`},
	}

	for _, tt := range cases {
		if got := quoteIdentifier(tt.in); got != tt.want {
			t.Fatalf("quoteIdentifier(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestQuoteTableName(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "deals", want: `"public"."deals"`},
		{in: "public.deals", want: `"public"."deals"`},
		{in: "myschema.deals", want: `"myschema"."deals"`},
		{in: "MySchema.Deals", want: `"MySchema"."Deals"`},
	}

	for _, tt := range cases {
		if got := quoteTableName(tt.in); got != tt.want {
			t.Fatalf("quoteTableName(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
