# erc-8004-benchmarking-be

Go backend for the ERC-8004 agent benchmarking platform. Runtime binaries are
organized under `cmd/workers/*`:

- `cmd/workers/api` — REST API + WebSocket fan-out (`/api/v1/*`, `/api/v1/ws`).
- `cmd/workers/event-decoder` — RabbitMQ consumer that decodes raw logs and writes events.
- `cmd/workers/indexer` — EVM log crawler that publishes raw logs to RabbitMQ.
- Additional workers: `cmd/workers/trustrank`, `cmd/workers/score-decay`, `cmd/workers/uri-bootstrap`.

## Realtime event stream

The platform broadcasts every successfully decoded event to connected WebSocket
clients. Flow:

```
EVM logs → indexer → RabbitMQ → event-decoder (decode + upsert) → Redis Pub/Sub
                                                             ↘
                                           API server subscribes & forwards
                                                             ↘
                                           WebSocket clients (/api/v1/ws)
```

Redis is used as a thin fan-out bus between the event-decoder and the API because the
two run as separate processes. Payload shape:

```json
{
  "type": "event.decoded",
  "eventName": "FeedbackCreated",
  "contractType": "reputation",
  "chainId": 8453,
  "agentId": "1435",
  "txHash": "0xabc…",
  "blockNumber": 12345,
  "logIndex": 2,
  "timestamp": 1770000000,
  "args": { "…": "…" }
}
```

Configure with `REDIS_URL` and `REDIS_EVENTS_CHANNEL` in `.env` (see
`.env.example`). Setting `REDIS_URL=""` disables realtime broadcast without
affecting REST endpoints.

## Local dev

```bash
cp .env.example .env
docker compose up -d mongo rabbitmq redis
go run ./cmd/workers/api
go run ./cmd/workers/event-decoder
go run ./cmd/workers/indexer
```
