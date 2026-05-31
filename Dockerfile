# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# OpenAPI / Swagger — cmd/workers/api embeds generated package swagger (see blank import in cmd/workers/api/main.go).
# swag CLI is pinned via the `tool` directive in go.mod; version stays in lockstep with the library require line.
RUN go tool swag init -g cmd/workers/api/main.go -o swagger --parseDependency --parseInternal

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/indexer ./cmd/workers/indexer
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/event-decoder ./cmd/workers/event-decoder
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/workers/api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/trustrank ./cmd/workers/trustrank
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/score-refresh ./cmd/workers/score-refresh
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/uri-bootstrap ./cmd/workers/uri-bootstrap
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/rescale ./cmd/workers/rescale
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/trust-graph-updater ./cmd/workers/trust-graph-updater
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/trustrank-pass ./cmd/workers/trustrank-pass
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/desc-summarizer ./cmd/workers/desc-summarizer
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/oasf-schema-refresh ./cmd/workers/oasf-schema-refresh

FROM alpine:3.20 AS base-runtime
RUN apk add --no-cache ca-certificates && adduser -D -H -u 65532 appuser
USER appuser:appuser

FROM base-runtime AS indexer
COPY --from=builder /out/indexer /usr/local/bin/indexer
ENTRYPOINT ["/usr/local/bin/indexer"]

# RabbitMQ consumer: event-decoder (compose service name: consumer).
FROM base-runtime AS consumer
COPY --from=builder /out/event-decoder /usr/local/bin/consumer
ENTRYPOINT ["/usr/local/bin/consumer"]

FROM base-runtime AS api
COPY --from=builder /out/api /usr/local/bin/api
ENTRYPOINT ["/usr/local/bin/api"]

FROM base-runtime AS trustrank-worker
COPY --from=builder /out/trustrank /usr/local/bin/trustrank-worker
ENTRYPOINT ["/usr/local/bin/trustrank-worker"]

FROM base-runtime AS score-worker
COPY --from=builder /out/score-refresh /usr/local/bin/score-worker
ENTRYPOINT ["/usr/local/bin/score-worker"]

FROM base-runtime AS uri-bootstrap
COPY --from=builder /out/uri-bootstrap /usr/local/bin/uri-bootstrap
ENTRYPOINT ["/usr/local/bin/uri-bootstrap"]

FROM base-runtime AS rescale-worker
COPY --from=builder /out/rescale /usr/local/bin/rescale-worker
ENTRYPOINT ["/usr/local/bin/rescale-worker"]

FROM base-runtime AS trust-graph-updater
COPY --from=builder /out/trust-graph-updater /usr/local/bin/trust-graph-updater
ENTRYPOINT ["/usr/local/bin/trust-graph-updater"]

FROM base-runtime AS trustrank-pass
COPY --from=builder /out/trustrank-pass /usr/local/bin/trustrank-pass
ENTRYPOINT ["/usr/local/bin/trustrank-pass"]

FROM base-runtime AS desc-summarizer
COPY --from=builder /out/desc-summarizer /usr/local/bin/desc-summarizer
ENTRYPOINT ["/usr/local/bin/desc-summarizer"]

FROM base-runtime AS oasf-schema-refresh
COPY --from=builder /out/oasf-schema-refresh /usr/local/bin/oasf-schema-refresh
ENTRYPOINT ["/usr/local/bin/oasf-schema-refresh"]

