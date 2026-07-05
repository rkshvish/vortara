# Build stage
FROM golang:1.25-alpine AS build
RUN apk add --no-cache build-base
WORKDIR /src

# Cache module downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO is required by the sqlite state backend (mattn/go-sqlite3);
# static-link against musl so the binary runs on scratch.
RUN CGO_ENABLED=1 go build -trimpath \
    -ldflags='-s -w -linkmode external -extldflags "-static"' \
    -o /vortara ./cmd/vortara

# Runtime stage — single static binary.
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /vortara /vortara

# State (sqlite) and DLQ files live under /data by convention;
# mount a volume and point settings.state.path there.
VOLUME ["/data"]
WORKDIR /data

ENTRYPOINT ["/vortara"]
CMD ["--help"]
