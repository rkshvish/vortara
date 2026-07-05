.PHONY: build test test-race test-integration vet clean fmt lint release-dry setup

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
