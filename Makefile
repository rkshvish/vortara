.PHONY: build test test-race test-integration vet clean fmt lint release-dry setup demo demo-clean

build:
	go build ./cmd/vortara

setup: build
	mkdir -p working-example/state
	docker compose -f working-example/docker-compose.yml up -d --build
	@echo "Setup complete."
	@echo "Run: ./vortara run working-example/pipeline.yaml"

test:
	GOCACHE=/tmp/vortara-cache go test ./...

test-race:
	GOCACHE=/tmp/vortara-cache go test -race ./...

test-e2e:
	GOCACHE=/tmp/vortara-cache go test -tags e2e -count=1 -v ./test/e2e/

test-integration:
	GOCACHE=/tmp/vortara-cache go test -tags integration ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

clean:
	rm -f vortara dist/

run:
	go run cmd/vortara/main.go

lint:
	golangci-lint run ./...

release-dry:
	@echo "Would release: $$(git describe --tags --always)"

# ── Demo: failure → DLQ → replay ────────────────────────────────────────────
#
# Prerequisites: Docker must be running.
# The demo spins up Postgres, seeds three leads, starts a local webhook server
# that intentionally returns 500 for lead_002, then runs through the full
# create → fail → DLQ → fix → replay → success cycle.
#
# Usage:
#   make demo          # run the full demo (idempotent — safe to re-run)
#   make demo-clean    # tear down Postgres and remove local state/dlq files

DEMO_DB_URL := postgres://vortara:vortara@localhost:15432/demo?sslmode=disable
DEMO_STATE  := ./state/demo-pql.db
DEMO_DLQ    := ./dlq/demo-pql.dlq.jsonl
DEMO_YAML   := ./demo/demo-sync.yaml
VORTARA     := go run ./cmd/vortara

demo: build demo-infra
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo " Vortara demo: failure → DLQ → replay"
	@echo "═══════════════════════════════════════════════════════════════"
	@echo ""
	$(eval export DEMO_DB_URL)

	@# ── Start webhook (lead_002 will fail with 500) ──────────────────
	@echo "→ Starting demo webhook (FAIL_KEYS=lead_002) on :18081"
	@pkill -f "demo/webhook" 2>/dev/null || true
	@FAIL_KEYS=lead_002 go run ./demo/webhook &
	@sleep 1

	@# ── Validate sync config ─────────────────────────────────────────
	@echo ""
	@echo "→ Validating sync config"
	@$(VORTARA) validate $(DEMO_YAML)

	@# ── Run YAML state tests ─────────────────────────────────────────
	@echo ""
	@echo "→ Running inline state tests"
	@$(VORTARA) test $(DEMO_YAML)

	@# ── First diff: should show create=3 ─────────────────────────────
	@echo ""
	@echo "→ Diff before first run (expect: creates=3)"
	@$(VORTARA) diff $(DEMO_YAML)

	@# ── First run: lead_001 + lead_003 succeed, lead_002 → DLQ ───────
	@echo ""
	@echo "→ Run (lead_002 webhook returns 500 → goes to DLQ)"
	@$(VORTARA) run $(DEMO_YAML) || true

	@# ── Show DLQ ─────────────────────────────────────────────────────
	@echo ""
	@echo "→ DLQ after run (expect: lead_002)"
	@$(VORTARA) dlq list $(DEMO_YAML)

	@# ── State inspect lead_002 (should show failed) ───────────────────
	@echo ""
	@echo "→ State inspect lead_002 (expect: status=failed)"
	@$(VORTARA) state inspect $(DEMO_YAML) lead_002

	@# ── Diff: lead_001 and lead_003 already delivered → skip ─────────
	@echo ""
	@echo "→ Diff after partial run (expect: creates=1 for lead_002 only)"
	@$(VORTARA) diff $(DEMO_YAML)

	@# ── Fix webhook: restart without FAIL_KEYS ───────────────────────
	@echo ""
	@echo "→ Fixing webhook (restarting without FAIL_KEYS)"
	@pkill -f "demo/webhook" 2>/dev/null || true
	@go run ./demo/webhook &
	@sleep 1

	@# ── Replay DLQ ───────────────────────────────────────────────────
	@echo ""
	@echo "→ Replay DLQ (lead_002 should now succeed)"
	@$(VORTARA) replay $(DEMO_YAML) --dlq

	@# ── State inspect lead_002 (should now show success) ─────────────
	@echo ""
	@echo "→ State inspect lead_002 after replay (expect: status=success)"
	@$(VORTARA) state inspect $(DEMO_YAML) lead_002

	@# ── Explain lead_002 ─────────────────────────────────────────────
	@echo ""
	@echo "→ Explain lead_002 (expect: decision=skip — already in state)"
	@$(VORTARA) explain $(DEMO_YAML) --key lead_002 || true

	@# ── Second run: all skip ─────────────────────────────────────────
	@echo ""
	@echo "→ Second full run (expect: all skip)"
	@$(VORTARA) run $(DEMO_YAML)

	@# ── Update lead_002 score, run again ─────────────────────────────
	@echo ""
	@echo "→ Updating lead_002 score in Postgres (82 → 95)"
	@PGPASSWORD=vortara psql -h localhost -p 15432 -U vortara -d demo \
	    -c "UPDATE leads SET lead_score=95, last_activity_at=now() WHERE id='lead_002';"

	@echo ""
	@echo "→ Diff after score change (expect: update=1 for lead_002)"
	@$(VORTARA) diff $(DEMO_YAML)

	@echo ""
	@echo "→ Run after score change"
	@$(VORTARA) run $(DEMO_YAML)

	@echo ""
	@echo "→ Explain lead_002 after update"
	@$(VORTARA) explain $(DEMO_YAML) --key lead_002 || true

	@# ── Clean up webhook ─────────────────────────────────────────────
	@pkill -f "demo/webhook" 2>/dev/null || true
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo " Demo complete."
	@echo "═══════════════════════════════════════════════════════════════"

demo-infra:
	@echo "→ Starting Postgres via Docker Compose"
	docker compose -f demo/docker-compose.yml up -d --wait
	@echo "→ Seeding demo data"
	@PGPASSWORD=vortara psql -h localhost -p 15432 -U vortara -d demo -f demo/seed/seed.sql
	@mkdir -p state dlq

demo-clean:
	@pkill -f "demo/webhook" 2>/dev/null || true
	docker compose -f demo/docker-compose.yml down -v
	rm -f $(DEMO_STATE) $(DEMO_DLQ)
