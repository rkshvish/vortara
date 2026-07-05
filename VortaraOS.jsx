import { useState } from "react";

const BLUE = "#1B4F8A";
const ACCENT = "#2E86AB";
const DARK = "#0D1117";
const SURFACE = "#161B22";
const CARD = "#1C2128";
const BORDER = "#30363D";
const TEXT = "#E6EDF3";
const MUTED = "#7D8590";
const GREEN = "#3FB950";
const YELLOW = "#D29922";
const RED = "#F85149";
const PURPLE = "#BC8CFF";

const TASKS = [
  {
    "id": "T001",
    "milestone": 1,
    "title": "Project scaffold + go.mod",
    "status": "done",
    "priority": "high",
    "component": "infra",
    "est": "2h",
    "desc": "Project path: /Users/rakesh_1/opensource/vortaraos \u2014 Initialize Go module, full folder structure, Makefile"
  },
  {
    "id": "T002",
    "milestone": 1,
    "title": "Standard Row struct",
    "status": "done",
    "priority": "high",
    "component": "core",
    "est": "1h",
    "desc": "pkg/row/row.go \u2014 ID, Source, Pipeline, PrimaryKey, Data, ExtractedAt, Watermark, Metadata"
  },
  {
    "id": "T003",
    "milestone": 1,
    "title": "YAML Config parser + validator",
    "status": "done",
    "priority": "high",
    "component": "config",
    "est": "4h",
    "desc": "pkg/config/config.go \u2014 parse pipeline YAML supporting mode: batch|streaming|both, validate fields, resolve env vars"
  },
  {
    "id": "T004",
    "milestone": 1,
    "title": "StateStore interface",
    "status": "done",
    "priority": "high",
    "component": "state",
    "est": "1h",
    "desc": "internal/state/store.go \u2014 GetWatermark, SetWatermark, GetOffset, SetOffset, StartRun, FinishRun, IsDelivered, MarkDelivered"
  },
  {
    "id": "T005",
    "milestone": 1,
    "title": "SQLite state implementation",
    "status": "done",
    "priority": "high",
    "component": "state",
    "est": "4h",
    "desc": "internal/state/sqlite.go \u2014 WAL mode, 4 tables: watermarks, kafka_offsets, run_log, delivery_log"
  },
  {
    "id": "T006",
    "milestone": 1,
    "title": "In-memory state (tests)",
    "status": "done",
    "priority": "medium",
    "component": "state",
    "est": "1h",
    "desc": "internal/state/memory.go \u2014 thread-safe map implementation for unit tests only"
  },
  {
    "id": "T007",
    "milestone": 2,
    "title": "BatchSource interface",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "1h",
    "desc": "internal/connector/source/batch.go \u2014 Connect, Extract(watermark), GetWatermarkColumn, Close"
  },
  {
    "id": "T008",
    "milestone": 2,
    "title": "Postgres batch source",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "4h",
    "desc": "SELECT WHERE updated_at > watermark. Cursor pagination. lib/pq driver."
  },
  {
    "id": "T009",
    "milestone": 2,
    "title": "REST API polling source",
    "status": "done",
    "priority": "medium",
    "component": "connector",
    "est": "3h",
    "desc": "GET with since= param or cursor pagination. Auth headers. Array + envelope response support."
  },
  {
    "id": "T009b",
    "milestone": 2,
    "title": "StreamingSource interface",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "1h",
    "desc": "internal/connector/source/streaming.go \u2014 Connect, Subscribe, Ack, Nack, Close"
  },
  {
    "id": "T009c",
    "milestone": 2,
    "title": "Kafka consumer source",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "5h",
    "desc": "Manual offset commits. Ack commits offset. Nack removes from pending without commit."
  },
  {
    "id": "T009d",
    "milestone": 2,
    "title": "Webhook receiver source",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "4h",
    "desc": "HTTP server. HMAC verification. Returns 200 only after Ack. 30s timeout returns 504."
  },
  {
    "id": "T010",
    "milestone": 2,
    "title": "Destination interface",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "1h",
    "desc": "internal/connector/destination/destination.go \u2014 Connect, Load(rows, store, pipeline, dest), Close"
  },
  {
    "id": "T011",
    "milestone": 2,
    "title": "REST API destination",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "4h",
    "desc": "POST/PUT per row. X-Idempotency-Key header. Retry 3x for 429/5xx. IsDelivered check before write."
  },
  {
    "id": "T012",
    "milestone": 2,
    "title": "Postgres destination",
    "status": "done",
    "priority": "medium",
    "component": "connector",
    "est": "3h",
    "desc": "INSERT ON CONFLICT upsert. Sorted column order. Parameterized values. IsDelivered + MarkDelivered."
  },
  {
    "id": "T013",
    "milestone": 3,
    "title": "Transformer \u2014 filter + enrich + map + drop",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "4h",
    "desc": "Apply() clones input. Filter conditions: row.field == value. Enrich: now(), row.field templates. Map renames. Drop removes."
  },
  {
    "id": "T014",
    "milestone": 3,
    "title": "Conditional router + fanout",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "2h",
    "desc": "Route() evaluates rules independently. Fanout() dispatches to N destinations concurrently via goroutines."
  },
  {
    "id": "T015",
    "milestone": 3,
    "title": "Extractor",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "2h",
    "desc": "Loads watermark, calls source, fixes Row.Pipeline on every row (THE authoritative fix), streams to channel."
  },
  {
    "id": "T016",
    "milestone": 3,
    "title": "Loader",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "2h",
    "desc": "Buffers 100 rows, applies transform+route+fanout, updates watermark only if non-zero, always calls FinishRun."
  },
  {
    "id": "T017",
    "milestone": 3,
    "title": "Core engine orchestrator",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "3h",
    "desc": "Wires all components. RunOnce for batch. StartStreaming for streaming. atomic.Bool prevents concurrent runs."
  },
  {
    "id": "T017b",
    "milestone": 3,
    "title": "StreamingRunner",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "4h",
    "desc": "Subscribe loop. ProcessOne per event. Ack on success, Nack on failure. Embedded in engine.go."
  },
  {
    "id": "T018",
    "milestone": 3,
    "title": "Scheduler (interval + cron)",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "3h",
    "desc": "robfig/cron/v3. Interval or cron, mutually exclusive. Skips if already running. RunNow for manual trigger."
  },
  {
    "id": "T019",
    "milestone": 4,
    "title": "CLI \u2014 run + validate",
    "status": "done",
    "priority": "high",
    "component": "cli",
    "est": "2h",
    "desc": "Cobra. validate prints errors. run loads config, builds engine, RunOnce, prints stats. Signal handling."
  },
  {
    "id": "T020",
    "milestone": 4,
    "title": "CLI \u2014 start + stop + status + history",
    "status": "done",
    "priority": "high",
    "component": "cli",
    "est": "2h",
    "desc": "start daemon with scheduler+streaming. status shows last run. history --limit N shows run log."
  },
  {
    "id": "T021",
    "milestone": 4,
    "title": "CLI \u2014 watermark + offset commands",
    "status": "done",
    "priority": "medium",
    "component": "cli",
    "est": "1h",
    "desc": "watermark get|reset and offset get|reset. Opens state store directly. GetRunHistory added to StateStore."
  },
  {
    "id": "T022",
    "milestone": 4,
    "title": "End-to-end integration test",
    "status": "done",
    "priority": "high",
    "component": "test",
    "est": "5h",
    "desc": "Real Postgres via testcontainers + httptest server. Batch flow, incremental watermark, idempotency, reset replay all verified."
  },
  {
    "id": "T023",
    "milestone": 5,
    "title": "Swap lib/pq to pgx/v5 in Postgres source",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "3h",
    "desc": "Replace lib/pq with jackc/pgx/v5/pgxpool. Native pool, correct pgtype handling, faster scan. Add quoteIdentifier helper. Update go.mod."
  },
  {
    "id": "T024",
    "milestone": 5,
    "title": "Schema introspection in Postgres source",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "4h",
    "desc": "Schema introspection before extraction. Query information_schema.columns + pg_index for PKs. Build typed column map. convertPgValue uses type info for correct int64/float64/bool/time.Time conversion. Fixes transform conditions like row.revenue > 1000."
  },
  {
    "id": "T025",
    "milestone": 5,
    "title": "Fix identifier quoting everywhere",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "1h",
    "desc": "Add quoteIdentifier() and quoteTableName() to Postgres source and destination. Fixes silent failures on mixed-case tables, reserved words, schema.table notation. Pattern: '\"' + strings.ReplaceAll(name, '\"', '\"\"') + '\"'. 1 hour fix, high impact."
  },
  {
    "id": "T026",
    "milestone": 5,
    "title": "Fix watermark query \u2014 IntervalStart + IntervalEnd",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "2h",
    "desc": "Change watermark WHERE from col > wm to col >= IntervalStart AND col <= IntervalEnd. Bounded window prevents full table scans on large tables. Update Extractor to compute and pass both bounds. Fix watermark advancement to use MAX(watermark_col) not time.Now()."
  },
  {
    "id": "T027",
    "milestone": 5,
    "title": "Swap lib/pq to pgx/v5 in Postgres destination",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "3h",
    "desc": "Replace lib/pq with pgx/v5/pgxpool. Use COPY protocol for bulk writes over 100 rows \u2014 10-100x faster than INSERT."
  },
  {
    "id": "T028",
    "milestone": 6,
    "title": "Add strategy field to YAML + engine",
    "status": "done",
    "priority": "high",
    "component": "config",
    "est": "4h",
    "desc": "Add strategy: merge|append|replace|delete+insert per destination in DestinationConfig. Default: merge. Pass to destination.Load(). Postgres: merge=ON CONFLICT UPDATE, append=INSERT IGNORE, replace=TRUNCATE+INSERT, delete+insert=DELETE+INSERT. Update YAML config and validator."
  },
  {
    "id": "T029",
    "milestone": 8,
    "title": "MySQL source connector",
    "status": "done",
    "priority": "medium",
    "component": "connector",
    "est": "5h",
    "desc": "internal/connector/source/mysql.go \u2014 go-sql-driver/mysql. Schema introspection from information_schema.columns. Backtick identifier quoting. Incremental: WHERE wm_col >= ? AND wm_col <= ? (positional params not $1). Batch via LIMIT/OFFSET. Same BatchSource interface as Postgres."
  },
  {
    "id": "T030",
    "milestone": 7,
    "title": "Snowflake source connector",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "5h",
    "desc": "internal/connector/source/snowflake.go \u2014 gosnowflake driver. Connect via DSN. Schema introspection from INFORMATION_SCHEMA.COLUMNS. Double-quote identifiers. Incremental query: WHERE wm_col >= ? AND wm_col <= ?. Batch via LIMIT/OFFSET. Warehouse, database, schema from connection string."
  },
  {
    "id": "T031",
    "milestone": 7,
    "title": "BigQuery source connector",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "6h",
    "desc": "internal/connector/source/bigquery.go \u2014 cloud.google.com/go/bigquery client. Service account auth via GOOGLE_APPLICATION_CREDENTIALS. Incremental via WHERE wm_col >= TIMESTAMP(?) AND wm_col <= TIMESTAMP(?). Batch via LIMIT/OFFSET on query. Project and dataset from connection string."
  },
  {
    "id": "T032",
    "milestone": 8,
    "title": "Snowflake destination connector",
    "status": "done",
    "priority": "medium",
    "component": "connector",
    "est": "5h",
    "desc": "internal/connector/destination/snowflake.go \u2014 gosnowflake driver. MERGE INTO for upsert (merge strategy). INSERT INTO for append. TRUNCATE + INSERT for replace. Stage file + COPY INTO for bulk loads >1000 rows. CREATE TABLE IF NOT EXISTS with correct Snowflake types."
  },
  {
    "id": "T033",
    "milestone": 6,
    "title": "Salesforce destination connector",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "6h",
    "desc": "internal/connector/destination/salesforce.go \u2014 OAuth2 client credentials flow. Upsert via /sobjects/{object}/{externalId}/{value}. Bulk API v2 for batches >200 rows: create job, upload CSV, close job, poll until complete. match_on = external ID field. Rate limit handling."
  },
  {
    "id": "T034",
    "milestone": 8,
    "title": "Slack destination connector",
    "status": "done",
    "priority": "medium",
    "component": "connector",
    "est": "2h",
    "desc": "internal/connector/destination/slack.go \u2014 incoming webhook POST. Message template with {{ row.field }} interpolation. cfg.Options[message] for template. Sends one message per row. Rate limit: 1 msg/sec. For high volume, consider digest mode (batch N rows into one message)."
  },
  {
    "id": "T035",
    "milestone": 6,
    "title": "Schema evolution \u2014 detect + handle column changes",
    "status": "parked",
    "priority": "high",
    "component": "engine",
    "est": "6h",
    "desc": "internal/engine/schemaevolution.go \u2014 detect when source schema changes (new col, dropped col, type change). Strategies: evolve (add new cols to dest), freeze (reject new cols), discard_row (skip rows with schema violations), discard_value (null out bad values). Config: schema_contract: evolve|freeze|discard_row|discard_value."
  },
  {
    "id": "T036",
    "milestone": 6,
    "title": "Schema inference for schema-less sources",
    "status": "parked",
    "priority": "medium",
    "component": "engine",
    "est": "4h",
    "desc": "internal/engine/schemainfer.go \u2014 for sources without fixed schema (REST API, webhooks, JSON). Infer schema from first N rows before writing. Track seen fields + types across batches. Widen types when conflicts found (int \u2192 float, any \u2192 string). Used by REST API source and Webhook source."
  },
  {
    "id": "T037",
    "milestone": 6,
    "title": "Chained transformer architecture",
    "status": "parked",
    "priority": "medium",
    "component": "engine",
    "est": "3h",
    "desc": "Refactor internal/transform/ to use Chain pattern from ingestr. Chain(filter, enrich, mapper, drop) returns single Transformer. Each step is independent, composable, testable. Makes adding new transform types trivial. No change to YAML config \u2014 only internal architecture."
  },
  {
    "id": "T038",
    "milestone": 7,
    "title": "Progress metrics tracker",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "4h",
    "desc": "internal/engine/progress.go \u2014 track rows/sec, total rows, total batches, duration, memory MB. Log every 5s during run via structured logger. Print final summary table after run: rows loaded, skipped, errors, duration, avg rows/sec. Add to RunStats struct so CLI can display it."
  },
  {
    "id": "T039",
    "milestone": 6,
    "title": "HTTP authenticator abstraction for REST sources/destinations",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "3h",
    "desc": "internal/connector/http/auth.go \u2014 Authenticator interface: Apply(req *http.Request), Name() string. Implementations: BearerAuth, APIKeyAuth (header or query), BasicAuth, OAuth2ClientCredentials (fetch+cache+refresh token). Used by REST source, REST dest, Salesforce, HubSpot. Config via auth: block in destination YAML."
  },
  {
    "id": "T040",
    "milestone": 6,
    "title": "Data buffer for schema-less sources",
    "status": "parked",
    "priority": "medium",
    "component": "engine",
    "est": "3h",
    "desc": "internal/engine/databuffer.go \u2014 in-memory buffer that accumulates rows before schema is known. Used when REST API or Webhook source needs to read N rows before inferring schema. Buffer rows, infer schema, then replay for writing. Prevents holding entire dataset in memory: spill to temp file if buffer exceeds 100MB."
  },
  {
    "id": "T041",
    "milestone": 8,
    "title": "Naming convention for column normalization",
    "status": "pending",
    "priority": "low",
    "component": "engine",
    "est": "2h",
    "desc": "internal/engine/naming.go \u2014 column name normalization. Modes: direct (no change, default), snake_case (CamelCase\u2192camel_case, spaces\u2192underscore), auto (detect and normalize). Config: schema_naming: direct|snake_case|auto in pipeline YAML. Applied in transformer before routing."
  },
  {
    "id": "T042",
    "milestone": 6,
    "title": "Interval tracker for delete+insert strategy",
    "status": "parked",
    "priority": "medium",
    "component": "engine",
    "est": "3h",
    "desc": "internal/engine/intervaltracker.go \u2014 for delete+insert strategy, track which time intervals have been loaded. Prevents deleting rows outside the current load window. Maintains interval_start + interval_end per pipeline run. Learned from ingestr strategy/interval_tracker.go pattern."
  },
  {
    "id": "T043",
    "milestone": 6,
    "title": "SCD2 (Slowly Changing Dimensions Type 2) strategy",
    "status": "done",
    "priority": "low",
    "component": "engine",
    "est": "8h",
    "desc": "Add strategy: scd2 to DestinationConfig. Adds _scd_valid_from, _scd_valid_to, _scd_is_current columns. On new row: close old version (set _scd_valid_to), insert new version. Requires Postgres destination first. Learned from ingestr SCD2Options + strategy/scd2.go pattern."
  },
  {
    "id": "T044",
    "milestone": 6,
    "title": "Postgres CDC source",
    "status": "done",
    "priority": "medium",
    "component": "connector",
    "est": "10h",
    "desc": "internal/connector/source/postgres_cdc.go \u2014 logical replication via pg_logical. Reads WAL stream. Emits INSERT/UPDATE/DELETE events as Rows with Operation field. Requires wal_level=logical on Postgres. Slot management, snapshot on first connect. Learned from ingestr pkg/source/postgres_cdc/ pattern."
  },
  {
    "id": "T045",
    "milestone": 8,
    "title": "Staging table pattern for atomic writes",
    "status": "done",
    "priority": "medium",
    "component": "connector",
    "est": "5h",
    "desc": "Staging table pattern for Postgres and Snowflake destinations with replace strategy. Write to temp table first, then atomic RENAME/SWAP into target. Prevents partial writes being visible. Postgres: ALTER TABLE RENAME. Snowflake: SWAP WITH. Config: atomic_swap: true in destination."
  },
  {
    "id": "T046",
    "milestone": 6,
    "title": "Multi-table CDC pipeline",
    "status": "parked",
    "priority": "low",
    "component": "engine",
    "est": "8h",
    "desc": "Support CDC sources that emit multiple tables simultaneously (postgres_cdc). Engine routes each table to its own destination config. Config: sources with tables: [orders, customers, products]. Learned from ingestr MultiTableSource interface + ReadAll pattern."
  },
  {
    "id": "T047",
    "milestone": 6,
    "title": "HubSpot destination connector",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "5h",
    "desc": "internal/connector/destination/hubspot.go \u2014 Private app token or OAuth2 auth. Batch upsert via /crm/v3/objects/{type}/batch/upsert (max 100 per batch). match_on maps to idProperty. Object types: contacts, companies, deals, tickets. Rate limit: 100 req/10s with backoff."
  },
  {
    "id": "T048",
    "milestone": 8,
    "title": "Google Sheets destination connector",
    "status": "done",
    "priority": "medium",
    "component": "connector",
    "est": "4h",
    "desc": "internal/connector/destination/googlesheets.go \u2014 Sheets API v4. Service account auth. Append rows to sheet (append only, no upsert). Spreadsheet ID and sheet name from config. Converts row.Data values to string for cells. Useful for non-technical stakeholders."
  },
  {
    "id": "T049",
    "milestone": 6,
    "title": "squirrel SQL query builder",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "2h",
    "desc": "Replace hand-built SQL strings in Postgres source and destination with github.com/Masterminds/squirrel. Eliminates string concatenation bugs, handles quoting automatically, makes strategy SQL (merge/append/replace) clean and readable. Learned from Bento's SQL output pattern."
  },
  {
    "id": "T050",
    "milestone": 6,
    "title": "Configurable retry + backoff per destination",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "2h",
    "desc": "Add retry config to DestinationConfig: retry_attempts (default 3), retry_backoff (default 1s), backoff_on (default [429,500,502,503,504]), drop_on (default [400,401,403,404]). Apply in REST destination and all future HTTP-based destinations. Learned from Bento httpclient config pattern."
  },
  {
    "id": "T051",
    "milestone": 8,
    "title": "Dead letter queue (on_error config)",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "4h",
    "desc": "Add on_error: skip|retry|dlq to pipeline config. skip=log and continue (current behavior). retry=retry N times then skip. dlq=send failed rows to a configured dead_letter destination. DLQ destination uses same Destination interface. Learned from Bento error handling strategy pattern."
  },
  {
    "id": "T052",
    "milestone": 8,
    "title": "Richer filter expression language",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "5h",
    "desc": "Extend filter condition parser to support: AND/OR/NOT operators (row.status == 'won' AND row.revenue > 1000), arithmetic (row.revenue * 0.1 > 100), string functions (contains(row.name, 'Corp'), startsWith(row.email, 'admin')). Keep simple \u2014 no full Bloblang. Extend current parser in internal/transform/filter.go. Inspired by Bento Bloblang concept."
  },
  {
    "id": "T053",
    "milestone": 6,
    "title": "Custom query mode for SQL sources",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "3h",
    "desc": "Add query: field to SourceConfig alongside table:. When query is set, execute it directly instead of building SELECT from table+watermark. Supports aggregations, joins, complex filters at source. Watermark injection: replace {{watermark}} placeholder in query with actual watermark value. Example: query: 'SELECT account_id, SUM(revenue) as total_revenue FROM deals WHERE updated_at > {{watermark}} GROUP BY account_id'. Implement in Postgres source first. Same pattern as ingestr IsCustomQuery."
  },
  {
    "id": "T054",
    "milestone": 6,
    "title": "Full refresh mode + dry run CLI flag",
    "status": "done",
    "priority": "high",
    "component": "cli",
    "est": "2h",
    "desc": "Add two CLI flags to vortara run: --full-refresh (ignore watermark, extract all rows) and --dry-run (extract + transform + route but skip actual destination writes, print what would be synced). Full refresh resets watermark to zero before run. Dry run prints rows per destination to stdout. Both critical for debugging and onboarding."
  },
  {
    "id": "T055",
    "milestone": 6,
    "title": "Connection test command",
    "status": "done",
    "priority": "high",
    "component": "cli",
    "est": "2h",
    "desc": "vortara test pipeline.yaml \u2014 validates config, then tests each connection: source Connect() + ping, each destination Connect() + ping. Prints pass/fail per connection with error details. No data moved. Essential for debugging credential issues before first run. Fast feedback loop for new users."
  },
  {
    "id": "T056",
    "milestone": 6,
    "title": "Column exclusion in source config",
    "status": "done",
    "priority": "medium",
    "component": "connector",
    "est": "2h",
    "desc": "Add exclude_columns: [col1, col2] to SourceConfig. Applied at extraction time \u2014 excluded columns never enter the pipeline. More efficient than drop transform (no data transferred). Critical for PII/sensitive columns (SSN, password_hash). Implement in Postgres source SELECT column list. Same pattern as ingestr ExcludeColumns in ReadOptions."
  },
  {
    "id": "T057",
    "milestone": 7,
    "title": "Health check + metrics HTTP endpoint",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "3h",
    "desc": "When vortara start is running: expose HTTP server on :9090 (configurable). GET /health \u2192 {status: ok, pipelines: [{name, mode, last_run, status}]}. GET /metrics \u2192 Prometheus-compatible text format: vortara_rows_loaded_total, vortara_rows_errored_total, vortara_last_run_duration_seconds, vortara_pipeline_up. Essential for production monitoring and colleague demo."
  },
  {
    "id": "T058",
    "milestone": 7,
    "title": "Event deduplication window for streaming",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "3h",
    "desc": "Add dedup_window: 5m to streaming config. Vortara tracks event IDs seen in the last N minutes in memory (LRU cache). Duplicate events within window are dropped before processing. Protects against at-least-once delivery from Kafka or webhook retries sending same event twice. Config: streaming.dedup_window: 5m (default: disabled)."
  },
  {
    "id": "T059",
    "milestone": 8,
    "title": "Backfill support \u2014 extract from date",
    "status": "done",
    "priority": "medium",
    "component": "cli",
    "est": "2h",
    "desc": "vortara run pipeline.yaml --since 2024-01-01 \u2014 override watermark with explicit start date for one run. Does not permanently change stored watermark. Useful for reprocessing historical data after a new destination is added. Implement as CLI flag that sets watermark override in RunOptions passed to Extractor."
  },
  {
    "id": "T060",
    "milestone": 8,
    "title": "Pipeline failure alerts via webhook",
    "status": "done",
    "priority": "low",
    "component": "engine",
    "est": "2h",
    "desc": "Add alerts: block to pipeline config. on_failure: webhook_url: ${SLACK_WEBHOOK}. On run failure: POST JSON payload to webhook with pipeline name, error, timestamp, rows_errored. Simple HTTP POST, same pattern as Slack destination. Config: alerts.on_failure.webhook_url. Also supports on_success for completion notifications."
  },
  {
    "id": "T061",
    "milestone": 8,
    "title": "Example pipeline library",
    "status": "done",
    "priority": "medium",
    "component": "infra",
    "est": "3h",
    "desc": "Create examples/ folder with real pipeline configs: postgres-to-salesforce.yaml, snowflake-to-hubspot.yaml, kafka-to-postgres.yaml, webhook-to-slack.yaml, bigquery-to-hubspot.yaml, postgres-aggregated-to-salesforce.yaml (custom query mode). Each example is a complete working config with comments. Critical for colleague onboarding and community adoption."
  },
  {
    "id": "T062",
    "milestone": 6,
    "title": "Documentation site",
    "status": "done",
    "priority": "high",
    "component": "infra",
    "est": "8h",
    "desc": "Create docs/ folder with markdown documentation. Structure: docs/getting-started.md (install, first pipeline in 5 min), docs/concepts.md (batch vs streaming vs RETL, watermark, strategy, idempotency), docs/yaml-reference.md (every field documented with examples), docs/sources/ (one page per source: postgres, restapi, kafka, webhook), docs/destinations/ (one page per destination: restapi, postgres, salesforce, hubspot, slack), docs/cli.md (every command with flags and examples), docs/examples/ (real pipeline walkthroughs). Deploy as GitHub Pages or Mintlify. Written in developer voice \u2014 no marketing fluff."
  },
  {
    "id": "T063",
    "milestone": 7,
    "title": "Worker pool for parallel transform + batch processing",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "5h",
    "desc": "Add parallelism: N to pipeline config (default: runtime.NumCPU()). Implement transform worker pool: N goroutines pulling rows from extraction channel, each worker runs transform+route independently, results fed into destination batcher. For streaming: N goroutines processing events concurrently from Subscribe channel. Uses sync.WaitGroup + buffered channels. Config: pipeline.parallelism: 4. Prevents CPU bottleneck on high-throughput pipelines and high event-rate streaming. Pattern from ingestr WriteOptions.Parallelism and Bento max_in_flight."
  },
  {
    "id": "T064",
    "milestone": 7,
    "title": "Streaming pipeline architecture \u2014 true concurrent stages",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "6h",
    "desc": "Refactor engine to run Extract, Transform, Load as concurrent pipeline stages connected by buffered channels. Stage 1: Extractor goroutine streams rows into chan(batch_size). Stage 2: N transform workers drain channel, apply transform+route, send to output chan(batch_size). Stage 3: Loader goroutine drains output channel, batches by destination, writes. All 3 stages run simultaneously \u2014 no stage waits for another to finish. Eliminates serial Extract\u2192wait\u2192Transform\u2192wait\u2192Load pattern. Maximum CPU and I/O overlap."
  },
  {
    "id": "T065",
    "milestone": 7,
    "title": "Memory-bounded extraction \u2014 fixed RAM ceiling",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "4h",
    "desc": "Enforce strict memory ceiling regardless of table size. Row channel is buffered at batch_size (default 1000). Extractor blocks when channel full \u2014 natural backpressure. Transform workers consume rows and free memory before next batch extracted. Memory formula: batch_size \u00d7 avg_row_size = constant. Test: 10M row table uses same RAM as 1000 row table. Add memory_limit_mb config option \u2014 if row size \u00d7 batch_size exceeds limit, reduce batch_size automatically."
  },
  {
    "id": "T066",
    "milestone": 7,
    "title": "Per-destination connection pool + write parallelism",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "4h",
    "desc": "Each destination gets its own connection pool with parallelism: N concurrent writers (default 3). Destination.Load() receives full batch, splits into N sub-batches, fires N goroutines each writing their sub-batch concurrently. Results merged back. Config per destination: parallelism: 5, batch_size: 200. Postgres uses pgxpool (already has this). REST/Salesforce/HubSpot get semaphore-controlled concurrent HTTP connections. Saturates destination API rate limits optimally."
  },
  {
    "id": "T067",
    "milestone": 7,
    "title": "Backpressure \u2014 flow control via buffered channels",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "3h",
    "desc": "Implement proper backpressure across all pipeline stages. When destination is slow (rate limited): Load goroutine blocks on write. Output channel fills up. Transform workers block on send. Input channel fills up. Extractor blocks on send to input channel. Net effect: extraction rate naturally matches delivery rate. No unbounded memory growth. Implement via correctly sized buffered channels + select with ctx.Done() at every stage. Add backpressure metrics: channel_depth gauge per stage."
  },
  {
    "id": "T068",
    "milestone": 6,
    "title": "Strategy registry pattern",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "3h",
    "desc": "Implement strategy registry like ingestr. var registry = map[string]LoadStrategy{}. Register() + Get() functions. Each strategy: Name(), Execute(ctx, job), Validate(cfg) error, RequiresPrimaryKey() bool. Replaces switch statement in engine. Makes adding new strategies trivial \u2014 just Register() in init(). Strategies: merge, append, replace, delete+insert. Clean separation from engine orchestration."
  },
  {
    "id": "T069",
    "milestone": 8,
    "title": "Column masking (PII redaction)",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "3h",
    "desc": "Add mask: [col1, col2] to transform config. Replaces specified column values with '***' or hash before delivery. Never touches source \u2014 only masks in pipeline before destination write. Critical for PII compliance (emails, SSNs, phone numbers). Learned from ingestr ColumnMasker transformer pattern. Config: transform: - mask: [email, phone, ssn]."
  },
  {
    "id": "T070",
    "milestone": 8,
    "title": "Whitespace trimmer transform",
    "status": "done",
    "priority": "low",
    "component": "engine",
    "est": "1h",
    "desc": "Add trim_whitespace: true to pipeline config. Strips leading/trailing whitespace from all string values before delivery. Prevents silent data quality issues from source systems with padded strings. Learned from ingestr WhitespaceTrimmer transformer. Config: pipeline.trim_whitespace: true."
  },
  {
    "id": "T071",
    "milestone": 7,
    "title": "Streaming flush interval + record threshold",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "3h",
    "desc": "For streaming mode: buffer events and flush to destination when EITHER flush_interval elapses OR flush_records threshold reached, whichever comes first. Prevents writing one event per API call. Config: streaming.flush_interval: 5s, streaming.flush_records: 100. After successful flush: commit Kafka offsets or ack webhooks. Learned from ingestr StreamingExecutor.FlushInterval + FlushRecords pattern."
  },
  {
    "id": "T072",
    "milestone": 6,
    "title": "Connector registry \u2014 auto-discover by type string",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "2h",
    "desc": "Implement connector registry like ingestr URI registry. Each connector registers itself in init(): source.Register('postgres', NewPostgresSource). engine.NewEngine() looks up by config type string instead of switch statement. Makes adding new connectors zero-touch to engine code. Both BatchSource and Destination registries. Config type: postgres \u2192 looks up registered PostgresSource automatically."
  },
  {
    "id": "T073",
    "milestone": 8,
    "title": "Query annotation for cost attribution",
    "status": "pending",
    "priority": "low",
    "component": "engine",
    "est": "2h",
    "desc": "Add annotations: {} to pipeline config. Key-value pairs injected as SQL comments into every query: -- @vortara.config: {pipeline: deals-sync, step: extract}. Enables cost attribution in Snowflake/BigQuery query history. Learned from ingestr annotation system (Prepend SQL comment pattern). Config: pipeline.annotations: {owner: data-team, pipeline: deals-sync}."
  },
  {
    "id": "T074",
    "milestone": 7,
    "title": "Rate limiter per destination",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "3h",
    "desc": "Add rate_limit to destination config: rate_limit: 100/10s (100 requests per 10 seconds). Token bucket implementation. Shared across all goroutines in destination worker pool. Prevents hitting Salesforce (100/10s), HubSpot (100/10s), Slack (1/s) rate limits. Learned from Bento rate_limit_local.go pattern. Config per destination: rate_limit: requests/period."
  },
  {
    "id": "T075",
    "milestone": 7,
    "title": "LRU cache for event dedup and lookup enrichment",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "3h",
    "desc": "Fixed-size LRU cache (hashicorp/golang-lru) for two use cases: 1) Event deduplication window (T058) \u2014 cache seen event IDs with TTL. 2) Lookup enrichment \u2014 cache results of DB lookups in enrich step (e.g. lookup account tier from DB, cache for 5min). Config: cache.size: 10000, cache.ttl: 5m. Learned from Bento LRU cache implementation."
  },
  {
    "id": "T076",
    "milestone": 8,
    "title": "SQLite-backed durable buffer for streaming",
    "status": "pending",
    "priority": "medium",
    "component": "engine",
    "est": "5h",
    "desc": "Optional durable buffer between source and destination for streaming mode. Events written to SQLite buffer first (persisted), then consumed and delivered, then deleted on ack. At-least-once guarantee even if destination is down for extended period. Config: streaming.buffer: sqlite, streaming.buffer_path: ./vortara/buffer.db. Learned from Bento SQLiteBuffer pattern using squirrel + cenkalti/backoff."
  },
  {
    "id": "T077",
    "milestone": 7,
    "title": "Row context \u2014 carry metadata through pipeline",
    "status": "done",
    "priority": "medium",
    "component": "core",
    "est": "2h",
    "desc": "Add context.Context to Row struct (like Bento message.Part). Allows pipeline metadata (trace IDs, origin pipeline, run ID) to flow with each row through all stages. Transform workers can read context for tracing. Destinations can add to outbound requests. Also enables per-row error attachment without changing Row.Data. Learned from Bento Part.WithContext pattern."
  },
  {
    "id": "T078",
    "milestone": 6,
    "title": "Smart strategy rewriting per destination type",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "2h",
    "desc": "Automatically rewrite strategies that are unsafe for specific destinations. Examples from ingestr: replace \u2192 truncate+insert for Postgres (DROP breaks dependent views/grants/FKs). merge required for CDC sources. Full refresh forces replace strategy. Implement in strategy registry as pre-Execute hook. Keeps YAML config simple while ensuring correct behavior per destination."
  },
  {
    "id": "T079",
    "milestone": 7,
    "title": "Circuit breaker per destination",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "3h",
    "desc": "Token bucket circuit breaker per destination. States: closed (normal), open (failing), half-open (testing). Opens after N consecutive failures (default 5). Stays open for cooldown period (default 30s). Half-open: try 1 request, if success \u2192 close, if fail \u2192 reopen. Prevents hammering failing Salesforce/HubSpot API causing cascading 429s. Config: destinations.sf.circuit_breaker.threshold: 5, cooldown: 30s."
  },
  {
    "id": "T080",
    "milestone": 7,
    "title": "Graceful shutdown \u2014 drain in-flight rows",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "2h",
    "desc": "On SIGTERM/SIGINT: stop accepting new extractions, wait for all in-flight rows to complete delivery, commit offsets/update watermarks, then exit cleanly. Timeout: 60s (configurable shutdown_timeout). After timeout: force exit and log undelivered rows. Currently engine just cancels context immediately losing in-flight data. Tested with kill -SIGTERM during active run."
  },
  {
    "id": "T081",
    "milestone": 7,
    "title": "Run timeout + max rows per run guard",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "2h",
    "desc": "Add max_runtime: 2h and max_rows: 1000000 to pipeline config. If run exceeds max_runtime: cancel context, save watermark to last successfully delivered row, log warning. If rows extracted exceeds max_rows: stop extraction, deliver buffered rows, save watermark. Prevents runaway batch jobs consuming all resources. Config: pipeline.max_runtime: 2h, pipeline.max_rows: 1000000."
  },
  {
    "id": "T082",
    "milestone": 7,
    "title": "Structured JSON logging",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "3h",
    "desc": "Replace fmt.Printf with structured logger (log/slog from Go 1.21 stdlib). Every log line includes: pipeline, run_id, row_id where applicable, level, timestamp, message. Log levels: DEBUG (per-row), INFO (per-batch), WARN (retries, skips), ERROR (failures). Output: JSON for production, text for terminal. Config: log.level: info, log.format: json|text. Zero external dependencies \u2014 stdlib slog."
  },
  {
    "id": "T083",
    "milestone": 8,
    "title": "OpenTelemetry traces",
    "status": "pending",
    "priority": "medium",
    "component": "engine",
    "est": "4h",
    "desc": "Add OpenTelemetry tracing via go.opentelemetry.io/otel. Spans: pipeline_run (root), extract (child), transform_batch (child), destination_write per dest (child). Attributes: pipeline name, run_id, rows_count, destination, strategy. Export to OTLP endpoint (Jaeger, Datadog, Honeycomb). Config: telemetry.otel_endpoint: http://localhost:4317. Off by default."
  },
  {
    "id": "T084",
    "milestone": 8,
    "title": "Docker image + docker-compose example",
    "status": "done",
    "priority": "high",
    "component": "infra",
    "est": "2h",
    "desc": "Official Dockerfile: FROM scratch (single binary), COPY vortara /vortara, ENTRYPOINT [/vortara]. Multi-arch build (amd64 + arm64) via GitHub Actions. Push to ghcr.io/rakesh/vortara:latest. docker-compose.yml example: vortara + postgres + kafka for local development. Publish to Docker Hub. Critical for adoption \u2014 most engineers evaluate via Docker first."
  },
  {
    "id": "T085",
    "milestone": 8,
    "title": "dbt integration \u2014 read dbt models as source",
    "status": "parked",
    "priority": "high",
    "component": "connector",
    "est": "5h",
    "desc": "Parse dbt manifest.json to discover models. Source type: dbt. Config: source.type: dbt, source.manifest: ./target/manifest.json, source.model: fct_won_deals. Translates dbt model \u2192 SQL query on the warehouse. Supports dbt profiles.yml for connection config. This is the flagship RETL use case: dbt model \u2192 Salesforce. Biggest differentiator vs Census/Hightouch."
  },
  {
    "id": "T086",
    "milestone": 8,
    "title": "Custom connector SDK",
    "status": "pending",
    "priority": "medium",
    "component": "infra",
    "est": "4h",
    "desc": "Public Go interfaces + registration API for community connectors. pkg/sdk/source.go: BatchSource + StreamingSource interfaces with helper utilities. pkg/sdk/destination.go: Destination interface + LoadResult helpers. Registration: sdk.RegisterSource('mydb', NewMyDBSource). Plugin loading: import _ 'github.com/user/vortara-mydb'. README with connector template. Enables community-built connectors without forking Vortara."
  },
  {
    "id": "T087",
    "milestone": 8,
    "title": "Pipeline dependencies \u2014 run B after A succeeds",
    "status": "pending",
    "priority": "medium",
    "component": "engine",
    "est": "3h",
    "desc": "Add depends_on: [pipeline-a] to pipeline config. Scheduler checks dependency status before running. Pipeline B only starts if pipeline A completed successfully in the same schedule window. Config: pipeline.depends_on: [raw-deals-sync]. Useful for: raw extract \u2192 transform model \u2192 RETL sync (dbt-style DAG). Simple linear dependencies only for MVP \u2014 no DAG complexity."
  },
  {
    "id": "T088",
    "milestone": 8,
    "title": "Secret manager integration",
    "status": "pending",
    "priority": "medium",
    "component": "config",
    "est": "3h",
    "desc": "Extend env var resolution to support secret manager URIs. Syntax: ${vault:secret/data/myapp#password}, ${aws:myapp/postgres#password}, ${env:POSTGRES_URL} (explicit). Providers: HashiCorp Vault (VAULT_ADDR + VAULT_TOKEN), AWS Secrets Manager (via AWS SDK), plain env var (default). Resolved at startup, not stored in state. Config stays clean \u2014 no secrets in YAML files."
  },
  {
    "id": "T089",
    "milestone": 7,
    "title": "GitHub Actions + Makefile CI targets",
    "status": "done",
    "priority": "medium",
    "component": "infra",
    "est": "2h",
    "desc": ".github/workflows/ci.yml: on push \u2192 make build + make test + make test-integration. Release workflow: on tag \u2192 cross-compile (linux/darwin/windows, amd64/arm64), upload to GitHub releases, push Docker image. Makefile targets: make release-dry, make release. goreleaser config for multi-platform binary distribution. Makes Vortara installable via: curl | bash or brew install."
  },
  {
    "id": "T090",
    "milestone": 7,
    "title": "Parallel extraction \u2014 split table by PK range",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "5h",
    "desc": "Split large table extraction across N goroutines by PK range. Step 1: SELECT MIN(id), MAX(id) to get range. Step 2: divide into extract_parallelism chunks. Step 3: N goroutines each run SELECT WHERE id BETWEEN $1 AND $2 AND updated_at > watermark concurrently. Results merged into single output channel. Config: pipeline.extract_parallelism: 4. Requires numeric primary key. Falls back to sequential if no PK. 3-4x throughput improvement on large tables."
  },
  {
    "id": "T091",
    "milestone": 7,
    "title": "Row object pool \u2014 zero GC pressure",
    "status": "done",
    "priority": "high",
    "component": "core",
    "est": "3h",
    "desc": "Use sync.Pool for row.Row objects to eliminate per-row allocations in the hot path. Pool pre-allocates Row + Data map. Worker gets Row from pool via row.Get(), fills fields, processes, then row.Put() returns to pool. Zero allocations during steady-state processing. Eliminates GC stop-the-world pauses during long runs. Benchmark before/after: target <1% GC overhead. Requires Row.Reset() method to clear fields before reuse."
  },
  {
    "id": "T092",
    "milestone": 7,
    "title": "Adaptive batch sizing \u2014 target memory per batch",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "3h",
    "desc": "Replace fixed batch_size with adaptive sizing. Measure avg_row_size from first 100 rows. Compute batch_size = target_batch_memory / avg_row_size (default target: 10MB). Clamp between min_batch: 100 and max_batch: 10000. Re-measure every 10 batches and adjust. Config: pipeline.batch_memory: 10MB (default), pipeline.batch_size: 1000 (override to disable adaptive). Prevents OOM on wide rows and maximizes throughput on narrow rows."
  },
  {
    "id": "T093",
    "milestone": 7,
    "title": "Prefetch next batch while loading current",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "4h",
    "desc": "Eliminate idle time between batches. While Loader writes current batch to destinations, Extractor pre-fetches next batch into prefetch buffer. When Loader finishes: swap buffers instantly, start loading prefetched batch immediately. Zero gap between batches. Prefetch buffer size = 1 batch (same memory as current batch). Net effect: Load latency completely hidden behind prefetch. Biggest single throughput improvement for I/O bound pipelines."
  },
  {
    "id": "T094",
    "milestone": 6,
    "title": "Postgres COPY protocol for bulk writes",
    "status": "done",
    "priority": "high",
    "component": "connector",
    "est": "4h",
    "desc": "Replace batch INSERT with pgx COPY protocol in Postgres destination for strategy: append and large merge staging. pgx CopyFrom() is 10-100x faster than INSERT for bulk loads. Flow: collect batch \u2192 pgx.CopyFromRows() \u2192 single COPY statement. For merge strategy: COPY into staging table, then MERGE staging \u2192 target. For append: COPY directly into target. Threshold: use COPY when batch_size > 100 rows. Fall back to INSERT for smaller batches or upsert conflicts. Already noted in T027 but needs dedicated implementation task."
  },
  {
    "id": "T095",
    "milestone": 8,
    "title": "TypedRow \u2014 replace map with []Value for CPU cache efficiency",
    "status": "parked",
    "priority": "medium",
    "component": "core",
    "est": "8h",
    "desc": "Replace row.Data map[string]interface{} with TypedRow using contiguous []Value array. Value struct: Type uint8, Int int64, Float float64, Str string \u2014 no pointer chasing. Schema *Schema shared across rows (stable pointer, stays in L3). Eliminates 20+ cache misses per row from map hash+pointer indirection. 5-10x transform throughput improvement. Requires updating all connectors, transformers, and destinations. Maintain backward-compatible NewRow() API with map for external connectors. TypedRow is internal fast path."
  },
  {
    "id": "T096",
    "milestone": 8,
    "title": "Columnar batch processing \u2014 Arrow-style column groups",
    "status": "parked",
    "priority": "low",
    "component": "core",
    "est": "12h",
    "desc": "Group rows by column for SIMD-friendly processing. Instead of row-by-row filter evaluation, evaluate filter condition across all rows for one column at a time. Column stays in L1/L2 cache during evaluation. For numeric comparisons (revenue > 10000): load all revenue values as []float64 \u2192 compare entire slice \u2192 bitmask result. 10-50x filter throughput on large batches. Requires TypedRow (T095) first. Post-MVP \u2014 significant architecture change."
  },
  {
    "id": "T097",
    "milestone": 8,
    "title": "Sort rows by PK before Postgres writes \u2014 B-tree locality",
    "status": "done",
    "priority": "low",
    "component": "connector",
    "est": "2h",
    "desc": "Sort []row.Row by PK value before writing to Postgres destination. B-tree inserts in PK order hit sequential leaf pages \u2192 pages stay in L3 cache \u2192 2-3x write throughput on large batches. Already sorted naturally when using parallel range extraction (T090). For non-sequential sources: sort before flushBatch(). Only applies to Postgres with integer PK. Cost: O(n log n) sort per batch \u2014 negligible vs I/O time."
  },
  {
    "id": "T098",
    "milestone": 8,
    "title": "Batch SQLite state writes \u2014 single transaction per run",
    "status": "done",
    "priority": "high",
    "component": "state",
    "est": "4h",
    "desc": "Group all MarkDelivered and watermark writes into a single SQLite transaction per pipeline run. Currently each MarkDelivered is a separate transaction = random disk writes. Batching all state writes: BEGIN \u2192 N MarkDelivered \u2192 SetWatermark \u2192 FinishRun \u2192 COMMIT = 1 sequential disk write. 10x SQLite throughput. Add StateStore.BeginBatch() / CommitBatch() methods. Loader accumulates state changes in memory during run, flushes atomically at end. WAL mode + batch = near-optimal disk I/O."
  },
  {
    "id": "T099",
    "milestone": 8,
    "title": "TCP buffer tuning for Postgres COPY and bulk transfers",
    "status": "parked",
    "priority": "medium",
    "component": "connector",
    "est": "2h",
    "desc": "Tune TCP send/receive buffers for Postgres connections used in COPY protocol. Default Go TCP buffer is 4KB. For COPY bulk writes: set SO_SNDBUF=262144 (256KB) via pgxpool.Config.ConnConfig.BuildFrontend or DialFunc. Larger buffer = fewer syscalls = less kernel overhead. Also set TCP_NODELAY for low-latency connections. Measure before/after with COPY of 100K rows. Only applies to Postgres destination COPY path."
  },
  {
    "id": "T100",
    "milestone": 8,
    "title": "HTTP/2 verification for REST destinations",
    "status": "done",
    "priority": "medium",
    "component": "connector",
    "est": "2h",
    "desc": "Verify Go http.Client uses HTTP/2 automatically for Salesforce and HubSpot (both support it). HTTP/2 multiplexes multiple requests over single TCP connection \u2014 reduces connection overhead for parallel writes. Add debug log: log which HTTP version is negotiated on first request. Add transport config: ForceAttemptHTTP2: true in http.Transport. For destinations not supporting HTTP/2: falls back to HTTP/1.1 automatically. Measure connection count before/after."
  },
  {
    "id": "T101",
    "milestone": 8,
    "title": "GOMAXPROCS tuning \u2014 physical cores vs hyperthreaded",
    "status": "done",
    "priority": "low",
    "component": "engine",
    "est": "2h",
    "desc": "Expose num_procs config in pipeline.concurrency.num_procs. Default 0 = Go runtime default (NumCPU = logical cores including HT). On HT systems: logical cores = 2x physical. Setting num_procs = physical cores reduces context switching between goroutines sharing the same physical core. Add: runtime.GOMAXPROCS(cfg.Pipeline.Concurrency.NumProcs) in engine init. Log effective GOMAXPROCS at startup. Benchmark: 8 logical (4 physical) \u2014 test both settings."
  },
  {
    "id": "T102",
    "milestone": 8,
    "title": "Postgres state backend",
    "status": "done",
    "priority": "high",
    "component": "state",
    "est": "5h",
    "desc": "internal/state/postgres.go \u2014 full StateStore implementation backed by Postgres. Tables: vortara_watermarks(pipeline,source,watermark), vortara_kafka_offsets(pipeline,topic,partition,offset), vortara_run_log(id,pipeline,mode,started_at,finished_at,rows_*,status,error), vortara_delivery_log(row_id,pipeline,destination,delivered_at). Uses pgx/v5 (already in go.mod). CREATE TABLE IF NOT EXISTS on first connect. Enables team deployments \u2014 multiple Vortara instances share state. Config: state.backend: postgres, state.connection: ${STATE_POSTGRES_URL}."
  },
  {
    "id": "T103",
    "milestone": 8,
    "title": "Redis state backend",
    "status": "pending",
    "priority": "medium",
    "component": "state",
    "est": "5h",
    "desc": "internal/state/redis.go \u2014 StateStore backed by Redis. Driver: go-redis/redis/v9. Keys: vortara:{pipeline}:watermark:{source} (string), vortara:{pipeline}:offset:{topic}:{partition} (string), vortara:{pipeline}:run:last (JSON hash), vortara:delivered:{row_id}:{pipeline}:{dest} (SETNX for atomic idempotency). Delivery log keys get configurable TTL (default 24h) \u2014 auto-expire old entries. SETNX on IsDelivered \u2192 atomic, no race condition. Ideal for high-throughput streaming where SQLite write latency is a bottleneck. Config: state.backend: redis, state.connection: ${REDIS_URL}, state.delivered_ttl: 24h."
  },
  {
    "id": "T104",
    "milestone": 8,
    "title": "State backend registry + config wiring",
    "status": "done",
    "priority": "high",
    "component": "state",
    "est": "2h",
    "desc": "internal/state/registry.go \u2014 same registry pattern as connector registry (T072). Register(backend string, factory StateStoreFactory). Factories: sqlite\u2192NewSQLiteStore, postgres\u2192NewPostgresStore, redis\u2192NewRedisStore, memory\u2192NewMemoryStore. Engine reads state.backend from config, calls registry.Get(backend)(cfg). Config validation: unknown backend \u2192 clear error with list of valid options. Default: sqlite. Update StateConfig in pkg/config/config.go to add backend, connection, delivered_ttl fields."
  },
  {
    "id": "T105",
    "milestone": 8,
    "title": "S3/GCS blob state backend",
    "status": "pending",
    "priority": "low",
    "component": "state",
    "est": "6h",
    "desc": "internal/state/blob.go \u2014 StateStore backed by object storage (S3 or GCS). State stored as JSON files: {prefix}/watermarks/{pipeline}/{source}.json, {prefix}/run_log/{pipeline}/latest.json, {prefix}/delivery_log/{pipeline}/{dest}/{row_id} (presence = delivered). Uses aws-sdk-go-v2 or cloud.google.com/go/storage. Eventual consistency \u2014 reads may lag writes by seconds. Suitable for serverless (Lambda, Cloud Run) where no persistent disk available. Config: state.backend: s3, state.bucket: my-bucket, state.prefix: vortara/, state.region: us-east-1."
  },
  {
    "id": "T106",
    "milestone": 8,
    "title": "YAML v2 config parser \u2014 name/from/also/steps/to/cron/settings",
    "status": "done",
    "priority": "high",
    "component": "config",
    "est": "10h",
    "desc": "New pkg/config/v2 package. Parse top-level keys: name, source, also, transform, destinations, cron, settings. Source + destination: type: field (not nested key). Auth inline per block. Three-level env resolution: (1) ${VAR_NAME} explicit reference, (2) VORTARA__{KEY_PATH} auto-mapped from YAML structure (source.url \u2192 VORTARA__SOURCE__URL, destinations[0].auth.client_id \u2192 VORTARA__DESTINATIONS__0__AUTH__CLIENT_ID), (3) literal YAML value. List fields comma-separated in env vars (VORTARA__SOURCE__EXCLUDE=ssn,credit_card). No backward compat \u2014 clean break from v1. vortara validate shows parsed config with resolved values (secrets masked)."
  },
  {
    "id": "T107",
    "milestone": 8,
    "title": "Steps processor \u2014 filter/rename/add/drop/mask in pipeline",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "6h",
    "desc": "Implement v2 steps processors: filter (drop row if condition false, same expr parser as existing), rename (map of old:new field names), add (map of field:expression \u2014 supports now(), if X then Y else Z, 'literal'), drop (list of field names to remove), mask (list of field names to replace with ***). Each step is a Go struct implementing StepProcessor interface: Apply(row.Row) (row.Row, bool). Chain runs in order. Replaces separate transform/filter/enrich/mapper/drop packages with unified steps."
  },
  {
    "id": "T108",
    "milestone": 8,
    "title": "v2 output block \u2014 to: list with when: routing",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "5h",
    "desc": "Parse to: as list of destination configs. Each item: source type as key (salesforce/hubspot/slack/postgres/snowflake), inline auth (no separate connections block), optional when: condition using same expression parser as filter. Engine evaluates when: per row per destination. No when: = always runs. Auth parsed inline: oauth2 fields directly under source type, bearer via token: field, webhook via webhook: field. Replaces destinations: map with flat list."
  },
  {
    "id": "T109",
    "milestone": 8,
    "title": "v2 YAML reference doc + migrate existing examples",
    "status": "done",
    "priority": "medium",
    "component": "infra",
    "est": "4h",
    "desc": "Rewrite docs/yaml-reference.md for v2 spec. Document all 7 top-level keys: name, from, also, steps, to, cron, settings. Per-key: all fields with types, defaults, examples. Migrate all examples/ to v2 YAML. Add migration guide: v1\u2192v2 mapping table. Update docs/getting-started.md and docs/concepts.md with v2 examples. Keep v1 examples in docs/examples/legacy/ for reference."
  },
  {
    "id": "T106",
    "milestone": 8,
    "title": "YAML v2 parser \u2014 name/source/steps/destinations/cron/settings",
    "status": "done",
    "priority": "high",
    "component": "config",
    "est": "8h",
    "desc": "New pkg/config/v2 package. Parse top-level keys: name, source, also, steps, destinations, cron, settings. Source block: detect connector type from nested key (postgres/kafka/webhook/snowflake/bigquery). Steps: filter/rename/add/drop/mask processors. Destinations: list with optional when: per item. Cron: parse shorthand (15m, 1h) + standard cron expression + manual. Settings: state/log/limits/on_error/concurrency. Backward compat: detect v1 YAML by presence of pipeline: key, auto-migrate to v2 struct. vortara validate shows which version detected."
  },
  {
    "id": "T107",
    "milestone": 8,
    "title": "Steps processor engine \u2014 filter/rename/add/drop/mask",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "5h",
    "desc": "Replace separate transform package with unified steps processor. Each step type: filter (drop row if condition false, supports AND/OR/NOT/contains/startsWith), rename (map of old:new field names), add (map of field:expression, supports now(), if/then/else, string literals), drop (list of field names to remove), mask (list of fields to replace with ***). Steps applied in order. Single Apply(row) method returns (row, keep bool). Replaces T013 transformer \u2014 same logic, cleaner API aligned to v2 YAML."
  },
  {
    "id": "T108",
    "milestone": 8,
    "title": "Destination when: routing \u2014 inline conditions per output",
    "status": "done",
    "priority": "high",
    "component": "engine",
    "est": "3h",
    "desc": "Each destination in destinations: list has optional when: condition. No when: = always receives row. when: evaluated per row using same expression engine as filter step. Replace separate router/fanout with destination-level when: evaluation. Engine iterates destinations list, evaluates when: per row, sends to matching destinations concurrently. Destinations without when: always run. One failure does not block others \u2014 each destination goroutine independent."
  },
  {
    "id": "T109",
    "milestone": 8,
    "title": "also: block \u2014 batch + streaming in same pipeline",
    "status": "done",
    "priority": "medium",
    "component": "engine",
    "est": "4h",
    "desc": "Support also: block alongside source: for batch+streaming mode. source: runs on cron schedule (batch). also: runs continuously (streaming). Both use same steps: and destinations:. Engine starts two goroutines: scheduler for batch, StreamingRunner for also. Both share state store but track watermarks and offsets independently. Config validation: also: only valid with kafka or webhook type. source: and also: can be different connector types."
  },
  {
    "id": "T110",
    "milestone": 8,
    "title": "Migrate all examples to v2 YAML",
    "status": "done",
    "priority": "medium",
    "component": "infra",
    "est": "3h",
    "desc": "Rewrite all example files in examples/ directory to v2 YAML format. Update docs/ to reflect v2 syntax. Update yaml-reference.md with complete v2 field reference. Add migration guide: docs/migrating-to-v2.md showing v1 \u2192 v2 side by side. Update README quick start example to v2. Examples: postgres-to-salesforce.yaml, snowflake-to-hubspot.yaml, kafka-to-hubspot.yaml, webhook-to-slack.yaml, bigquery-to-hubspot.yaml, postgres-aggregated-to-salesforce.yaml."
  }
];

const DECISIONS = [
  {
    "id": "D001",
    "area": "Language",
    "decision": "Go",
    "reason": "Goroutines for concurrent fan-out, single binary deploy, excellent Postgres/REST libs"
  },
  {
    "id": "D002",
    "area": "ETL Mode",
    "decision": "Batch + Streaming (Kafka + Webhook)",
    "reason": "Batch for scheduled syncs with watermark, streaming for real-time via Kafka consumer or Webhook receiver"
  },
  {
    "id": "D003",
    "area": "Incremental Strategy",
    "decision": "Watermark on updated_at column",
    "reason": "Simple, universal, works on every DB. No special DB config required."
  },
  {
    "id": "D004",
    "area": "State Store",
    "decision": "SQLite with WAL mode (default)",
    "reason": "Zero deps, single binary, crash safe, SQL for debugging, swappable via interface"
  },
  {
    "id": "D005",
    "area": "Idempotency",
    "decision": "Delivery log + upsert at destination",
    "reason": "Delivery log prevents duplicate loads, upsert makes re-runs safe"
  },
  {
    "id": "D006",
    "area": "Config",
    "decision": "YAML-first, git-native",
    "reason": "Developer-first DX, version controllable, matches dbt mental model"
  },
  {
    "id": "D007",
    "area": "MVP Sources",
    "decision": "Postgres + REST API polling",
    "reason": "Covers 80% of real use cases. Postgres most common, REST covers any API."
  },
  {
    "id": "D008",
    "area": "MVP Destinations",
    "decision": "REST API + Postgres",
    "reason": "REST covers Salesforce/HubSpot/any API. Postgres covers DB-to-DB use cases."
  },
  {
    "id": "D009",
    "area": "CLI Framework",
    "decision": "Cobra",
    "reason": "Industry standard Go CLI, used by Docker/Kubernetes, excellent subcommand support"
  },
  {
    "id": "D010",
    "area": "Scheduler",
    "decision": "Interval + cron, one run at a time",
    "reason": "Simple, predictable, no run overlap. Missed runs skipped, not queued."
  },
  {
    "id": "D011",
    "area": "Streaming Sources",
    "decision": "Kafka consumer + Webhook receiver (MVP)",
    "reason": "Kafka for teams already on Kafka. Webhook for SaaS sources. CDC post-MVP."
  },
  {
    "id": "D012",
    "area": "Pipeline Mode",
    "decision": "mode: batch | streaming | both per pipeline",
    "reason": "Both mode runs streaming continuously + batch on schedule for reconciliation."
  },
  {
    "id": "D013",
    "area": "Postgres Driver",
    "decision": "pgx/v5 replacing lib/pq",
    "reason": "Native connection pooling, correct pgtype handling, faster scan, actively maintained. Matches ingestr pattern."
  },
  {
    "id": "D014",
    "area": "Write Strategy",
    "decision": "strategy: append|merge|replace|delete+insert",
    "reason": "Learned from ingestr. merge=upsert (default), append=insert only, replace=drop+insert, delete+insert=delete matching+insert."
  },
  {
    "id": "D015",
    "area": "Product Focus",
    "decision": "Batch + Streaming + Reverse ETL",
    "reason": "Three equal pillars: Batch (scheduled extraction+load), Streaming (real-time Kafka/Webhook events), Reverse ETL (warehouse/DB \u2192 operational tools like Salesforce, HubSpot, Slack). All three use the same transform+route+destination engine."
  },
  {
    "id": "D016",
    "area": "SQL Query Building",
    "decision": "squirrel library for all SQL construction",
    "reason": "Eliminates string concatenation bugs and quoting issues. Handles ON CONFLICT, INSERT, SELECT, UPDATE cleanly. Learned from Bento SQL output. Replaces hand-built SQL strings in Postgres source and destination."
  },
  {
    "id": "D017",
    "area": "Error Handling",
    "decision": "on_error: skip|retry|dlq per pipeline",
    "reason": "Production pipelines need control over failure behavior. skip=log+continue, retry=N attempts, dlq=route failed rows to dead letter destination. Learned from Bento error handling strategy."
  },
  {
    "id": "D018",
    "area": "Aggregations",
    "decision": "Aggregations belong at source via custom query, not in transformer",
    "reason": "80% of RETL is row-level sync. For aggregations (SUM, COUNT, GROUP BY), user writes SQL at source. Vortara syncs pre-aggregated rows. This is how dbt+Census/Hightouch work. Keeps transformer stateless and simple. Custom query mode (T053) unlocks this pattern."
  },
  {
    "id": "D019",
    "area": "CPU + Parallelism",
    "decision": "Worker pool with parallelism: N config (default: NumCPU)",
    "reason": "Transform is embarrassingly parallel. Single goroutine bottlenecks high-throughput batch and high-event-rate streaming. Worker pool with configurable N goroutines saturates available CPU cores. Destination fanout already parallel. Pattern from ingestr Parallelism and Bento max_in_flight."
  },
  {
    "id": "D020",
    "area": "Pipeline Architecture",
    "decision": "Three concurrent stages: Extract \u2192 Transform pool \u2192 Load, connected by buffered channels",
    "reason": "Eliminates serial wait between stages. All three run simultaneously. Extract fills channel while Transform drains it while Load writes. Maximum CPU and I/O overlap. Memory ceiling = buffer_size \u00d7 row_size = constant regardless of table size."
  },
  {
    "id": "D021",
    "area": "Memory Management",
    "decision": "Fixed memory ceiling via buffered channel backpressure",
    "reason": "Row channel buffered at batch_size. Extractor blocks when full. Transform workers consume before next batch extracted. Memory = batch_size \u00d7 row_size = constant. 10M row table uses same RAM as 1K row table. Prevents OOM on large tables."
  },
  {
    "id": "D022",
    "area": "Destination Parallelism",
    "decision": "Per-destination connection pool with configurable parallelism",
    "reason": "Each destination gets N concurrent writers (default 3). Saturates destination API rate limits optimally. Postgres uses pgxpool. REST destinations use semaphore-controlled concurrent HTTP. Config: destinations.salesforce.parallelism: 5."
  },
  {
    "id": "D023",
    "area": "Strategy Pattern",
    "decision": "Strategy registry with Register()/Get() \u2014 no switch statements",
    "reason": "Learned from ingestr strategy registry. Each strategy registers in init(). Engine looks up by name. Adding new strategies requires zero changes to engine code. Clean separation of concerns."
  },
  {
    "id": "D024",
    "area": "Connector Pattern",
    "decision": "Connector registry \u2014 auto-discover by type string",
    "reason": "Learned from ingestr URI registry. source.Register('postgres', NewPostgresSource) in connector init(). Engine.NewEngine() looks up registered connector by config.source.type string. Zero engine changes to add new connectors."
  },
  {
    "id": "D025",
    "area": "Rate Limiting",
    "decision": "Token bucket rate limiter per destination",
    "reason": "Salesforce: 100 req/10s. HubSpot: 100 req/10s. Slack: 1 msg/s. Without rate limiting, parallel destination writes will hit API limits causing 429 errors and cascading failures. Token bucket shared across all goroutines in destination pool."
  },
  {
    "id": "D026",
    "area": "Streaming Buffer",
    "decision": "Streaming flush on interval OR record count threshold",
    "reason": "Learned from ingestr StreamingExecutor. Flush when EITHER flush_interval (5s) OR flush_records (100) threshold reached. Prevents 1-event-per-API-call at low volume and excessive latency at high volume. After flush: commit offsets/ack webhooks."
  },
  {
    "id": "D027",
    "area": "PII Safety",
    "decision": "Column masking in transform layer before destination",
    "reason": "PII (email, SSN, phone) must never reach destinations unmasked. Masking in transform layer is the right place \u2014 after source extraction, before destination delivery. Source data untouched. Learned from ingestr ColumnMasker pattern."
  },
  {
    "id": "D028",
    "area": "Logging",
    "decision": "stdlib slog for structured JSON logging \u2014 zero external deps",
    "reason": "Go 1.21 stdlib slog gives structured logging with zero dependencies. JSON output for production log aggregation (Datadog, Loki). Text output for terminal. Every log line has pipeline, run_id context. No zerolog/zap dependency needed."
  },
  {
    "id": "D029",
    "area": "Reliability",
    "decision": "Circuit breaker per destination \u2014 open/half-open/closed states",
    "reason": "Salesforce/HubSpot APIs fail intermittently. Without circuit breaker: all goroutines pile up waiting on failing API, consuming resources and burning retry budget. Circuit breaker stops attempting after N failures, reopens after cooldown."
  },
  {
    "id": "D030",
    "area": "Ecosystem",
    "decision": "dbt integration as flagship RETL source",
    "reason": "dbt \u2192 Salesforce/HubSpot is the #1 RETL use case. Every data team uses dbt. Reading from dbt manifest.json positions Vortara as the natural complement to dbt. Bigger differentiator than any individual connector."
  },
  {
    "id": "D031",
    "area": "Extraction Performance",
    "decision": "Parallel extraction by PK range split \u2014 N goroutines each own a range",
    "reason": "Single goroutine extraction leaves DB connection pool and CPU idle. Splitting by PK range (id BETWEEN) allows N concurrent SELECT queries. 3-4x throughput on large tables. Requires numeric PK. Falls back to sequential gracefully."
  },
  {
    "id": "D032",
    "area": "Memory Performance",
    "decision": "sync.Pool for Row objects \u2014 zero allocation in hot path",
    "reason": "1M rows without pooling = 1M allocations = GC pressure = stop-the-world pauses during runs. sync.Pool pre-allocates and reuses Row + map objects. Zero allocations in steady state. GC overhead drops from ~5% to <1% of runtime."
  },
  {
    "id": "D033",
    "area": "Batch Sizing",
    "decision": "Adaptive batch sizing targeting 10MB per batch",
    "reason": "Fixed batch_size is wrong for all row sizes except the one it was tuned for. Adaptive sizing measures avg_row_size from first 100 rows and computes batch_size = 10MB / avg_row_size. Always optimal memory utilization regardless of schema width."
  },
  {
    "id": "D034",
    "area": "I/O Overlap",
    "decision": "Prefetch next batch while loading current \u2014 zero idle time between batches",
    "reason": "Without prefetch: Load(200ms) \u2192 Extract(100ms) \u2192 Load(200ms). 33% of time is idle. With prefetch: Load and Extract run simultaneously. Extract latency completely hidden. Single biggest throughput gain for I/O bound pipelines."
  },
  {
    "id": "D035",
    "area": "Postgres Write Performance",
    "decision": "COPY protocol for bulk writes > 100 rows \u2014 10-100x faster than INSERT",
    "reason": "INSERT is row-by-row even when batched as VALUES. COPY is a streaming bulk protocol with no per-row overhead. pgx CopyFromRows() handles this natively. For append strategy: COPY directly. For merge: COPY to staging then MERGE. Fall back to INSERT for small batches."
  },
  {
    "id": "D036",
    "area": "CPU Cache Optimization",
    "decision": "Three-level cache strategy: Pool reuse (L1) \u2192 sequential slices (L2) \u2192 TypedRow (post-MVP)",
    "reason": "sync.Pool (T091) keeps row memory in L1/L2 between iterations. []row.Row slice is contiguous (cache-friendly vs pointer arrays). map[string]interface{} causes cache misses \u2014 TypedRow (T095) is the post-MVP fix. Arrow columnar (T096) maximizes L1 for SIMD-width operations."
  },
  {
    "id": "D037",
    "area": "Row Data Model",
    "decision": "map[string]interface{} for MVP, TypedRow []Value post-MVP",
    "reason": "map is flexible and correct. TypedRow is 5-10x faster but requires updating all connectors, transformers, and destinations \u2014 a large architectural change. MVP correctness first, TypedRow in M8 after all connectors are stable."
  },
  {
    "id": "D038",
    "area": "Resource Utilization Summary",
    "decision": "CPU Cores 85%, CPU Cache 60%, SIMD 0%, RAM 75%, Disk 50%, Network 70%, OS Scheduler 80%",
    "reason": "CPU cores saturated via worker pool + parallel stages + parallel extraction. CPU cache limited by map[string]interface{} \u2014 TypedRow (T095) is biggest remaining win. SIMD blocked on TypedRow. RAM bounded via channels + pool + adaptive sizing. Disk bottleneck is per-row SQLite writes \u2014 T098 batches them. Network needs TCP buffer tuning (T099) and HTTP/2 (T100). OS scheduler needs GOMAXPROCS physical core tuning (T101)."
  },
  {
    "id": "D039",
    "area": "Disk I/O Strategy",
    "decision": "Batch all SQLite state writes in single transaction per run",
    "reason": "Per-row MarkDelivered = random disk writes = 50% disk utilization. Single transaction per run = 1 sequential write = 10x throughput. WAL mode + batch transaction = near-optimal SQLite performance. State consistency maintained \u2014 if run fails mid-way, transaction rolls back."
  },
  {
    "id": "D040",
    "area": "Network Strategy",
    "decision": "HTTP/2 for all REST destinations + TCP buffer 256KB for Postgres COPY",
    "reason": "HTTP/2 multiplexes parallel requests over single TCP connection \u2014 reduces connection overhead for Salesforce/HubSpot parallel writes. Go http.Client uses HTTP/2 automatically when server supports it. Postgres COPY with 256KB TCP buffer reduces syscall overhead 64x vs default 4KB."
  },
  {
    "id": "D041",
    "area": "State Backend",
    "decision": "Pluggable state via StateStore interface + registry \u2014 sqlite|postgres|redis|s3",
    "reason": "StateStore interface already exists. SQLite is default (zero deps, single machine). Postgres enables team deployments (shared state, multiple instances). Redis enables high-throughput streaming (sub-ms reads, atomic SETNX, TTL on delivery log). S3/GCS enables serverless (no persistent disk). Same registry pattern as connector registry \u2014 zero engine changes to add new backends."
  },
  {
    "id": "D042",
    "area": "YAML Config v2",
    "decision": "name / source / also / transform / destinations / cron / settings \u2014 auth inline per block",
    "reason": "Final locked spec. No top-level credentials block. Auth inline inside source or destination with type: oauth2/bearer/api_key/basic fields. type: field (not nested key) for all connectors. No backward compat \u2014 clean break. Seven top-level keywords: name, source, also, transform, destinations, cron, settings."
  },
  {
    "id": "D042",
    "area": "YAML Config v2",
    "decision": "name / source / also / steps / destinations / cron / settings \u2014 seven keywords",
    "reason": "Inspired by Bento (input/pipeline/output mental model). Simplified to developer-first vocabulary. source = where data comes from (one). destinations = where data goes (list, optional when: per item). steps = ordered processors (filter/rename/add/drop/mask). cron = schedule with shorthand support. also = optional streaming alongside batch. settings = all operational config. Backward compat: v1 YAML auto-detected and migrated."
  },
  {
    "id": "D043",
    "area": "Environment Variable Interpolation",
    "decision": "Three-level resolution: ${VAR} explicit \u2192 VORTARA__{KEY_PATH} auto-mapped \u2192 literal YAML value",
    "reason": "Developers can use explicit ${VAR} references OR auto-mapped VORTARA__SOURCE__URL style without any ${} in YAML. Auto-mapping uses double-underscore separator, UPPERCASE, array index for destinations[0]. List fields comma-separated. Enables same pipeline.yaml deployed to multiple environments with only env vars changing. No secrets in YAML files needed."
  }
];

const MILESTONES = [
  {
    "id": 1,
    "title": "Foundation",
    "desc": "Row struct, Config parser, StateStore (SQLite)",
    "target": "Week 1 \u2705"
  },
  {
    "id": 2,
    "title": "Connectors",
    "desc": "Source + Destination interfaces and first implementations",
    "target": "Week 2 \u2705"
  },
  {
    "id": 3,
    "title": "Core Engine",
    "desc": "Transform, Router, Extractor, Loader, Scheduler",
    "target": "Week 3 \u2705"
  },
  {
    "id": 4,
    "title": "Working Draft",
    "desc": "CLI, end-to-end test, real pipeline flowing",
    "target": "Week 4 \u2705"
  },
  {
    "id": 5,
    "title": "Tier 1 \u2014 Fix Broken",
    "desc": "Schema introspection, identifier quoting, watermark bounds. Fixes correctness on real data.",
    "target": "Week 5 \u00b7 7h total"
  },
  {
    "id": 6,
    "title": "Tier 2 \u2014 Real Pipelines",
    "desc": "HTTP auth, strategy, Salesforce + HubSpot destinations. First real RETL pipelines.",
    "target": "Week 6-7 \u00b7 18h total"
  },
  {
    "id": 7,
    "title": "Tier 3 \u2014 Warehouse Sources",
    "desc": "Snowflake + BigQuery sources, progress metrics. Flagship warehouse\u2192SaaS use case.",
    "target": "Week 8 \u00b7 15h total"
  },
  {
    "id": 8,
    "title": "Tier 4 \u2014 Round Out",
    "desc": "Slack, Google Sheets, Snowflake dest, MySQL, naming, staging tables.",
    "target": "Week 9-10 \u00b7 23h total"
  }
];

const INTERFACES = [
  {
    "name": "BatchSource",
    "file": "internal/connector/source/batch.go",
    "color": "#2E86AB",
    "methods": [
      "Connect(ctx context.Context, cfg config.SourceConfig) error",
      "Extract(ctx context.Context, watermark time.Time, out chan<- row.Row) error",
      "GetWatermarkColumn() string",
      "Close() error"
    ]
  },
  {
    "name": "StreamingSource",
    "file": "internal/connector/source/streaming.go",
    "color": "#BC8CFF",
    "methods": [
      "Connect(ctx context.Context, cfg config.StreamingConfig) error",
      "Subscribe(ctx context.Context, out chan<- row.Row) error",
      "Ack(ctx context.Context, rowID string) error",
      "Nack(ctx context.Context, rowID string) error",
      "Close() error"
    ]
  },
  {
    "name": "Destination",
    "file": "internal/connector/destination/destination.go",
    "color": "#FF7B72",
    "methods": [
      "Connect(ctx context.Context, cfg config.DestinationConfig) error",
      "Load(ctx context.Context, rows []row.Row, store state.StateStore, pipeline, destination string) (LoadResult, error)",
      "Close() error"
    ]
  },
  {
    "name": "StateStore",
    "file": "internal/state/store.go",
    "color": "#3FB950",
    "methods": [
      "GetWatermark(pipeline, source string) (time.Time, error)",
      "SetWatermark(pipeline, source string, wm time.Time) error",
      "GetOffset(pipeline, topic string, partition int) (int64, error)",
      "SetOffset(pipeline, topic string, partition int, offset int64) error",
      "StartRun(pipeline, mode string) (int64, error)",
      "FinishRun(runID int64, stats RunStats) error",
      "GetLastRun(pipeline string) (RunLog, error)",
      "GetRunHistory(pipeline string, limit int) ([]RunLog, error)",
      "IsDelivered(rowID, pipeline, destination string) (bool, error)",
      "MarkDelivered(rowID, pipeline, destination string) error",
      "Close() error"
    ]
  }
];

const SQLITE_TABLES = [
  {
    "name": "watermarks",
    "pk": "(pipeline, source)",
    "cols": "watermark DATETIME, updated_at DATETIME",
    "purpose": "Batch: where each pipeline left off"
  },
  {
    "name": "kafka_offsets",
    "pk": "(pipeline, topic, partition)",
    "cols": "offset BIGINT, updated_at DATETIME",
    "purpose": "Streaming: committed Kafka offset per partition"
  },
  {
    "name": "run_log",
    "pk": "id AUTOINCREMENT",
    "cols": "pipeline, mode, started_at, finished_at, rows stats, status, error",
    "purpose": "Audit trail of every run"
  },
  {
    "name": "delivery_log",
    "pk": "(row_id, pipeline, destination)",
    "cols": "delivered_at DATETIME",
    "purpose": "Idempotency for batch and streaming"
  }
];

const RUN_SEQUENCE = [
  [
    "1",
    "Load Watermark",
    "Read last MAX(updated_at) from state store"
  ],
  [
    "2",
    "Extract",
    "SELECT WHERE updated_at > watermark, batch by batch"
  ],
  [
    "3",
    "Transform",
    "Filter then Enrich then Map then Drop per row"
  ],
  [
    "4",
    "Route",
    "Evaluate conditions, fan out to N destinations"
  ],
  [
    "5",
    "Load",
    "Batch upsert, check IsDelivered first"
  ],
  [
    "6",
    "Update Watermark",
    "Save MAX(updated_at) from this batch"
  ],
  [
    "7",
    "Log Run",
    "Write RunStats to run_log table"
  ]
];

const DEV_PROMPT = "You are a senior Go engineer building Vortara.\\n\\nProject:\\n  Path:   /Users/rakesh_1/opensource/vortaraos\\n  Module: github.com/rakesh/vortaraos\\n\\nCore Rules:\\n- Idiomatic Go: interfaces, errors as values, context propagation\\n- All errors handled explicitly, no panic\\n- No global mutable state\\n- Godoc on all exported symbols\\n- Table-driven tests\\n\\nInterfaces:\\n  BatchSource: Connect, Extract(watermark), GetWatermarkColumn, Close\\n  StreamingSource: Connect, Subscribe, Ack, Nack, Close\\n  Destination: Connect, Load(ctx, rows, store, pipeline, dest), Close\\n  StateStore: GetWatermark, SetWatermark, GetOffset, SetOffset, StartRun, FinishRun, IsDelivered, MarkDelivered, Close\\n\\nRun Sequence (never change):\\n  1. Load watermark\\n  2. Extract rows WHERE updated_at > watermark\\n  3. Transform (filter, enrich, map, drop)\\n  4. Route to destinations by condition\\n  5. Load with IsDelivered check\\n  6. Update watermark\\n  7. Log run\\n\\nCurrent Task:\\n{PASTE_TASK_HERE}";
const QA_PROMPT = "You are a QA engineer testing Vortara.\\n\\nPriorities:\\n1. Watermark correctness\\n2. Idempotency (no duplicates)\\n3. Transform correctness\\n4. Routing correctness\\n5. Error handling\\n6. Scheduler safety\\n\\nAlways check:\\n- Watermark advances after successful run\\n- Same data twice = zero duplicates at destination\\n- IsDelivered checked before write, MarkDelivered after\\n- No goroutine leaks\\n- Run with -race flag\\n\\nComponent to test:\\n{PASTE_COMPONENT_HERE}";
const REVIEW_PROMPT = "You are a principal engineer reviewing Vortara code.\\n\\nChecklist:\\n- BatchSource/StreamingSource/Destination interfaces correct\\n- StateStore used, no direct SQLite calls outside state package\\n- Row.Pipeline set by extractor, not connector\\n- Watermark updated AFTER successful delivery\\n- IsDelivered before write, MarkDelivered after success\\n- No goroutine leaks, all goroutines exit on ctx cancel\\n- No data races\\n- Batch size respected\\n- Godoc on exports\\n\\nSeverity:\\n  CRITICAL: watermark wrong, data duplicated/lost\\n  HIGH: goroutine leak, wrong interface\\n  MEDIUM: missing error handling\\n  LOW: style, naming\\n\\nCode to review:\\n{PASTE_CODE_HERE}";
const PROGRESS_PROMPT = "You are reviewing Vortara engineering progress.\\n\\nAssess:\\n1. Does each completed task implement the correct interface?\\n2. Is watermark handling correct?\\n3. Any idempotency gaps?\\n4. Architecture drift from decisions?\\n5. Critical path to next milestone?\\n\\nCompleted tasks:\\n{PASTE_COMPLETED_TASKS}\\n\\nOutput:\\n### What is Working\\n### Risks and Issues\\n### Blockers\\n### Revised Timeline\\n### Next 3 Tasks";

const PROMPTS = {
  developer: { title: "Developer Prompt", icon: "⚡", color: GREEN, description: "Use when asking AI to write Vortara code", content: DEV_PROMPT },
  qa:        { title: "QA Prompt", icon: "🧪", color: YELLOW, description: "Use when asking AI to write tests", content: QA_PROMPT },
  review:    { title: "Code Review Prompt", icon: "🔍", color: PURPLE, description: "Use when asking AI to review code", content: REVIEW_PROMPT },
  progress:  { title: "Progress Check Prompt", icon: "📊", color: ACCENT, description: "Use for CTO-level status report", content: PROGRESS_PROMPT },
};

const COMPONENTS = ["all","core","state","connector","engine","config","cli","infra","test"];
const STATUSES = ["all","pending","in-progress","done","blocked","parked"];

function Badge({ text }) {
  const colors = {
    high: { bg: "#3D1A1A", text: RED }, medium: { bg: "#2D2A0F", text: YELLOW }, low: { bg: "#0D2818", text: GREEN },
    pending: { bg: "#1C2128", text: MUTED }, "in-progress": { bg: "#1A2A3D", text: ACCENT },
    done: { bg: "#0D2818", text: GREEN }, blocked: { bg: "#3D1A1A", text: RED }, parked: { bg: "#1A1A1A", text: "#555555" },
    core: { bg: "#1A1A3D", text: PURPLE }, state: { bg: "#1A2A1A", text: GREEN },
    connector: { bg: "#2A1A1A", text: "#FF7B72" }, engine: { bg: "#1A2A3D", text: ACCENT },
    config: { bg: "#2A2A1A", text: YELLOW }, cli: { bg: "#1A2A2A", text: "#79C0FF" },
    infra: { bg: "#2A1A2A", text: PURPLE }, test: { bg: "#1A3D1A", text: GREEN },
  };
  const c = colors[text] || { bg: "#1C2128", text: MUTED };
  return <span style={{ background: c.bg, color: c.text, padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.5, fontFamily: "monospace" }}>{text}</span>;
}

function Tab({ label, active, onClick, count }) {
  return (
    <button onClick={onClick} style={{ background: active ? ACCENT : "transparent", color: active ? "#fff" : MUTED, border: "none", padding: "8px 16px", borderRadius: 6, cursor: "pointer", fontSize: 13, fontWeight: active ? 600 : 400, display: "flex", alignItems: "center", gap: 6 }}>
      {label}
      {count !== undefined && <span style={{ background: active ? "rgba(255,255,255,0.2)" : BORDER, color: active ? "#fff" : MUTED, borderRadius: 10, padding: "1px 6px", fontSize: 11 }}>{count}</span>}
    </button>
  );
}

function CopyButton({ text }) {
  const [copied, setCopied] = useState(false);
  return (
    <button onClick={() => { navigator.clipboard.writeText(text.replace(/\\n/g, "\n")); setCopied(true); setTimeout(() => setCopied(false), 2000); }}
      style={{ background: copied ? "#0D2818" : SURFACE, color: copied ? GREEN : MUTED, border: "1px solid " + (copied ? GREEN : BORDER), borderRadius: 6, padding: "6px 14px", cursor: "pointer", fontSize: 12, fontWeight: 600 }}>
      {copied ? "✓ Copied" : "Copy Prompt"}
    </button>
  );
}

function TaskBoard({ tasks, setTasks }) {
  const [filterMilestone, setFilterMilestone] = useState("all");
  const [filterComponent, setFilterComponent] = useState("all");
  const [filterStatus, setFilterStatus] = useState("all");
  const [selected, setSelected] = useState(null);

  const filtered = tasks.filter(t =>
    (filterMilestone === "all" || t.milestone === parseInt(filterMilestone)) &&
    (filterComponent === "all" || t.component === filterComponent) &&
    (filterStatus === "all" || t.status === filterStatus)
  );

  const cycleStatus = (id) => {
    const cycle = { pending: "in-progress", "in-progress": "done", done: "blocked", blocked: "pending" };
    setTasks(prev => prev.map(t => t.id === id ? { ...t, status: cycle[t.status] } : t));
  };

  const stats = {
    total: tasks.filter(t => t.status !== "parked").length,
    done: tasks.filter(t => t.status === "done").length,
    inProgress: tasks.filter(t => t.status === "in-progress").length,
    blocked: tasks.filter(t => t.status === "blocked").length,
  };

  const pct = Math.round((stats.done / stats.total) * 100);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 12 }}>
        {[["Total", stats.total, TEXT],["Done", stats.done, GREEN],["In Progress", stats.inProgress, ACCENT],["Blocked", stats.blocked, RED]].map(([l,v,c]) => (
          <div key={l} style={{ background: CARD, border: "1px solid " + BORDER, borderRadius: 8, padding: "12px 16px" }}>
            <div style={{ color: MUTED, fontSize: 11, marginBottom: 4, textTransform: "uppercase", letterSpacing: 0.5 }}>{l}</div>
            <div style={{ color: c, fontSize: 24, fontWeight: 700, fontFamily: "monospace" }}>{v}</div>
          </div>
        ))}
      </div>
      <div style={{ background: CARD, border: "1px solid " + BORDER, borderRadius: 8, padding: "12px 16px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
          <span style={{ color: MUTED, fontSize: 12 }}>Overall Progress</span>
          <span style={{ color: TEXT, fontSize: 12, fontWeight: 600 }}>{pct}%</span>
        </div>
        <div style={{ background: BORDER, borderRadius: 4, height: 6 }}>
          <div style={{ background: GREEN, borderRadius: 4, height: 6, width: pct + "%", transition: "width 0.3s" }} />
        </div>
      </div>
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        <select value={filterMilestone} onChange={e => setFilterMilestone(e.target.value)} style={{ background: CARD, color: TEXT, border: "1px solid " + BORDER, borderRadius: 6, padding: "6px 10px", fontSize: 12 }}>
          <option value="all">All Milestones</option>
          {MILESTONES.map(m => <option key={m.id} value={m.id}>M{m.id}: {m.title}</option>)}
        </select>
        <select value={filterComponent} onChange={e => setFilterComponent(e.target.value)} style={{ background: CARD, color: TEXT, border: "1px solid " + BORDER, borderRadius: 6, padding: "6px 10px", fontSize: 12 }}>
          {COMPONENTS.map(c => <option key={c} value={c}>{c === "all" ? "All Components" : c}</option>)}
        </select>
        <select value={filterStatus} onChange={e => setFilterStatus(e.target.value)} style={{ background: CARD, color: TEXT, border: "1px solid " + BORDER, borderRadius: 6, padding: "6px 10px", fontSize: 12 }}>
          {STATUSES.map(s => <option key={s} value={s}>{s === "all" ? "All Status" : s}</option>)}
        </select>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {filtered.map(task => (
          <div key={task.id} style={{ background: CARD, border: "1px solid " + (selected === task.id ? ACCENT : BORDER), borderRadius: 8, padding: "12px 16px", cursor: "pointer" }} onClick={() => setSelected(selected === task.id ? null : task.id)}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
              <span style={{ color: MUTED, fontSize: 11, fontFamily: "monospace", minWidth: 48 }}>{task.id}</span>
              <span style={{ color: TEXT, fontSize: 13, fontWeight: 500, flex: 1 }}>{task.title}</span>
              <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                <Badge text={task.component} /><Badge text={task.priority} /><Badge text={task.status} />
                <span style={{ color: MUTED, fontSize: 11, fontFamily: "monospace" }}>M{task.milestone}</span>
                <span style={{ color: MUTED, fontSize: 11 }}>{task.est}</span>
                <button onClick={e => { e.stopPropagation(); cycleStatus(task.id); }} style={{ background: SURFACE, border: "1px solid " + BORDER, color: MUTED, borderRadius: 4, padding: "3px 8px", cursor: "pointer", fontSize: 11 }}>↻</button>
              </div>
            </div>
            {selected === task.id && <div style={{ marginTop: 10, paddingTop: 10, borderTop: "1px solid " + BORDER, color: MUTED, fontSize: 12, lineHeight: 1.6 }}>{task.desc}</div>}
          </div>
        ))}
      </div>
    </div>
  );
}

function MilestoneView({ tasks }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {MILESTONES.map(m => {
        const mTasks = tasks.filter(t => t.milestone === m.id);
        const done = mTasks.filter(t => t.status === "done").length;
        const pct = mTasks.length ? Math.round((done / mTasks.length) * 100) : 0;
        return (
          <div key={m.id} style={{ background: CARD, border: "1px solid " + BORDER, borderRadius: 10, padding: 20 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 12 }}>
              <div>
                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 4 }}>
                  <span style={{ background: BLUE, color: "#fff", borderRadius: 4, padding: "2px 8px", fontSize: 11, fontWeight: 700, fontFamily: "monospace" }}>M{m.id}</span>
                  <span style={{ color: TEXT, fontSize: 16, fontWeight: 600 }}>{m.title}</span>
                  <span style={{ color: MUTED, fontSize: 12 }}>→ {m.target}</span>
                </div>
                <div style={{ color: MUTED, fontSize: 12 }}>{m.desc}</div>
              </div>
              <div style={{ textAlign: "right" }}>
                <div style={{ color: pct === 100 ? GREEN : TEXT, fontSize: 22, fontWeight: 700, fontFamily: "monospace" }}>{pct}%</div>
                <div style={{ color: MUTED, fontSize: 11 }}>{done}/{mTasks.length} done</div>
              </div>
            </div>
            <div style={{ background: BORDER, borderRadius: 4, height: 4, marginBottom: 12 }}>
              <div style={{ background: pct === 100 ? GREEN : ACCENT, borderRadius: 4, height: 4, width: pct + "%", transition: "width 0.3s" }} />
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {mTasks.map(t => (
                <div key={t.id} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ color: t.status === "done" ? GREEN : t.status === "in-progress" ? ACCENT : t.status === "blocked" ? RED : MUTED, fontSize: 14 }}>
                    {t.status === "done" ? "✓" : t.status === "in-progress" ? "◉" : t.status === "blocked" ? "✕" : t.status === "parked" ? "⊘" : "○"}
                  </span>
                  <span style={{ color: t.status === "done" ? MUTED : TEXT, fontSize: 12, textDecoration: t.status === "done" ? "line-through" : "none" }}>{t.title}</span>
                  <span style={{ color: MUTED, fontSize: 11, fontFamily: "monospace", marginLeft: "auto" }}>{t.est}</span>
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function PromptsView() {
  const [active, setActive] = useState("developer");
  const p = PROMPTS[active];
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        {Object.entries(PROMPTS).map(([key, val]) => (
          <button key={key} onClick={() => setActive(key)} style={{ background: active === key ? val.color + "22" : "transparent", color: active === key ? val.color : MUTED, border: "1px solid " + (active === key ? val.color : BORDER), borderRadius: 8, padding: "8px 16px", cursor: "pointer", fontSize: 13, fontWeight: active === key ? 600 : 400 }}>
            {val.icon} {val.title}
          </button>
        ))}
      </div>
      <div style={{ background: CARD, border: "1px solid " + BORDER, borderRadius: 10, padding: 20 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 12 }}>
          <div>
            <div style={{ color: p.color, fontSize: 16, fontWeight: 700, marginBottom: 4 }}>{p.icon} {p.title}</div>
            <div style={{ color: MUTED, fontSize: 12 }}>{p.description}</div>
          </div>
          <CopyButton text={p.content} />
        </div>
        <div style={{ background: SURFACE, border: "1px solid " + BORDER, borderRadius: 8, padding: 16, maxHeight: 420, overflow: "auto" }}>
          <pre style={{ margin: 0, color: TEXT, fontSize: 12, lineHeight: 1.7, fontFamily: "monospace", whiteSpace: "pre-wrap", wordBreak: "break-word" }}>{p.content.replace(/\n/g, "\n")}</pre>
        </div>
        <div style={{ marginTop: 12, padding: "10px 14px", background: "#1A2A1A", border: "1px solid " + GREEN + "33", borderRadius: 6 }}>
          <span style={{ color: GREEN, fontSize: 11, fontWeight: 600 }}>HOW TO USE: </span>
          <span style={{ color: MUTED, fontSize: 11 }}>Copy then paste into Cursor or Claude Code and replace the placeholder at the bottom with your specific task or code.</span>
        </div>
      </div>
    </div>
  );
}

function DecisionsView() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <div style={{ color: MUTED, fontSize: 12, marginBottom: 4 }}>{DECISIONS.length} locked architectural decisions. Reference before writing any code.</div>
      {DECISIONS.map(d => (
        <div key={d.id} style={{ background: CARD, border: "1px solid " + BORDER, borderRadius: 8, padding: "14px 16px" }}>
          <div style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
            <span style={{ color: MUTED, fontSize: 11, fontFamily: "monospace", minWidth: 44 }}>{d.id}</span>
            <div style={{ flex: 1 }}>
              <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 4 }}>
                <span style={{ color: MUTED, fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5 }}>{d.area}</span>
                <span style={{ color: TEXT, fontSize: 13, fontWeight: 600, fontFamily: "monospace" }}>{d.decision}</span>
              </div>
              <div style={{ color: MUTED, fontSize: 12, lineHeight: 1.6 }}>{d.reason}</div>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

function CodeLogicView() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div style={{ color: TEXT, fontSize: 14, fontWeight: 600 }}>Go Interfaces</div>
      {INTERFACES.map(iface => (
        <div key={iface.name} style={{ background: CARD, border: "1px solid " + BORDER, borderRadius: 10, padding: 16 }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 10 }}>
            <div style={{ color: iface.color, fontSize: 14, fontWeight: 700, fontFamily: "monospace" }}>type {iface.name} interface</div>
            <span style={{ color: MUTED, fontSize: 11, fontFamily: "monospace" }}>{iface.file}</span>
          </div>
          <div style={{ background: SURFACE, borderRadius: 6, padding: 12 }}>
            {iface.methods.map((m, i) => <div key={i} style={{ color: TEXT, fontSize: 12, fontFamily: "monospace", lineHeight: 1.8, paddingLeft: 16 }}>{m}</div>)}
          </div>
        </div>
      ))}
      <div style={{ color: TEXT, fontSize: 14, fontWeight: 600, marginTop: 8 }}>Pipeline Run Sequence (Sacred)</div>
      <div style={{ background: CARD, border: "1px solid " + BORDER, borderRadius: 10, padding: 16 }}>
        {RUN_SEQUENCE.map(([num, title, desc]) => (
          <div key={num} style={{ display: "flex", gap: 12, marginBottom: 10, alignItems: "flex-start" }}>
            <span style={{ background: BLUE, color: "#fff", borderRadius: 4, padding: "2px 7px", fontSize: 11, fontWeight: 700, fontFamily: "monospace", minWidth: 20, textAlign: "center" }}>{num}</span>
            <div>
              <div style={{ color: TEXT, fontSize: 12, fontWeight: 600 }}>{title}</div>
              <div style={{ color: MUTED, fontSize: 11 }}>{desc}</div>
            </div>
          </div>
        ))}
      </div>
      <div style={{ color: TEXT, fontSize: 14, fontWeight: 600, marginTop: 8 }}>SQLite Tables (4 total)</div>
      <div style={{ background: CARD, border: "1px solid " + BORDER, borderRadius: 10, padding: 16 }}>
        {SQLITE_TABLES.map((t, i) => (
          <div key={t.name} style={{ marginBottom: i < SQLITE_TABLES.length - 1 ? 14 : 0, paddingBottom: i < SQLITE_TABLES.length - 1 ? 14 : 0, borderBottom: i < SQLITE_TABLES.length - 1 ? "1px solid " + BORDER : "none" }}>
            <div style={{ color: GREEN, fontSize: 12, fontWeight: 700, fontFamily: "monospace", marginBottom: 4 }}>{t.name}</div>
            <div style={{ color: MUTED, fontSize: 11, marginBottom: 2 }}>PK: {t.pk}</div>
            <div style={{ color: MUTED, fontSize: 11, marginBottom: 2 }}>Cols: {t.cols}</div>
            <div style={{ color: TEXT, fontSize: 11 }}>→ {t.purpose}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default function VortaraOS() {
  const [tasks, setTasks] = useState(TASKS);
  const [activeTab, setActiveTab] = useState("tasks");
  const tabs = [
    { id: "tasks", label: "Tasks", count: tasks.filter(t => t.status !== "done").length },
    { id: "milestones", label: "Milestones" },
    { id: "prompts", label: "AI Prompts" },
    { id: "decisions", label: "Decisions", count: DECISIONS.length },
    { id: "logic", label: "Code Logic" },
  ];
  return (
    <div style={{ background: DARK, minHeight: "100vh", color: TEXT, fontFamily: "-apple-system, BlinkMacSystemFont, \'Segoe UI\', sans-serif" }}>
      <div style={{ background: SURFACE, borderBottom: "1px solid " + BORDER, padding: "14px 24px", display: "flex", alignItems: "center", gap: 16 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ background: BLUE, borderRadius: 8, width: 32, height: 32, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 16 }}>🌀</div>
          <div>
            <div style={{ color: TEXT, fontWeight: 700, fontSize: 15, letterSpacing: 0.5 }}>VORTARA</div>
            <div style={{ color: MUTED, fontSize: 10, letterSpacing: 1, textTransform: "uppercase" }}>Batch · Streaming · Reverse ETL</div>
          </div>
        </div>
        <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
          {[["CTO", BLUE],["Architect", ACCENT],["TPM", PURPLE]].map(([r,c]) => (
            <span key={r} style={{ background: c + "22", color: c, border: "1px solid " + c + "44", borderRadius: 4, padding: "3px 8px", fontSize: 11, fontWeight: 600 }}>{r}</span>
          ))}
        </div>
      </div>
      <div style={{ background: SURFACE, borderBottom: "1px solid " + BORDER, padding: "8px 24px", display: "flex", gap: 4 }}>
        {tabs.map(t => <Tab key={t.id} label={t.label} active={activeTab === t.id} onClick={() => setActiveTab(t.id)} count={t.count} />)}
      </div>
      <div style={{ padding: "24px", maxWidth: 900, margin: "0 auto" }}>
        {activeTab === "tasks" && <TaskBoard tasks={tasks} setTasks={setTasks} />}
        {activeTab === "milestones" && <MilestoneView tasks={tasks} />}
        {activeTab === "prompts" && <PromptsView />}
        {activeTab === "decisions" && <DecisionsView />}
        {activeTab === "logic" && <CodeLogicView />}
      </div>
    </div>
  );
}
