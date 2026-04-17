# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# OpenAPI / Swagger — cmd/api embeds generated package docs/swagger (see blank import in cmd/api/main.go).
# docs/ is gitignored locally; image build must regenerate so Docker builds work from a clean clone.
RUN go install github.com/swaggo/swag/cmd/swag@v1.16.6 && \
	/go/bin/swag init -g cmd/api/main.go -o docs/swagger --parseDependency --parseInternal

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/indexer ./cmd/indexer
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/trustrank-worker ./cmd/trustrank-worker
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/decay-worker ./cmd/decay-worker
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/uri-bootstrap ./cmd/uri-bootstrap

FROM alpine:3.20 AS base-runtime
RUN apk add --no-cache ca-certificates && adduser -D -H -u 65532 appuser
USER appuser:appuser

FROM base-runtime AS indexer
COPY --from=builder /out/indexer /usr/local/bin/indexer
ENTRYPOINT ["/usr/local/bin/indexer"]

FROM base-runtime AS consumer
COPY --from=builder /out/consumer /usr/local/bin/consumer
ENTRYPOINT ["/usr/local/bin/consumer"]

FROM base-runtime AS api
COPY --from=builder /out/api /usr/local/bin/api
ENTRYPOINT ["/usr/local/bin/api"]

FROM base-runtime AS trustrank-worker
COPY --from=builder /out/trustrank-worker /usr/local/bin/trustrank-worker
ENTRYPOINT ["/usr/local/bin/trustrank-worker"]

FROM base-runtime AS decay-worker
COPY --from=builder /out/decay-worker /usr/local/bin/decay-worker
ENTRYPOINT ["/usr/local/bin/decay-worker"]

FROM base-runtime AS uri-bootstrap
COPY --from=builder /out/uri-bootstrap /usr/local/bin/uri-bootstrap
ENTRYPOINT ["/usr/local/bin/uri-bootstrap"]

