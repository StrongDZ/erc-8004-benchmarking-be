# erc-8004-benchmarking-be

Go backend for the ERC-8004 agent benchmarking platform. Ships as three cooperating
processes plus infra:

- `cmd/api` — REST API + WebSocket fan-out (`/api/v1/*`, `/api/v1/ws`).
- `cmd/consumer` — RabbitMQ consumer that decodes raw logs and writes events.
- `cmd/crawler` — EVM log crawler that publishes raw logs to RabbitMQ.
- Workers: `cmd/trustrank-worker`, `cmd/decay-worker`, `cmd/uri-bootstrap`, ...

## Realtime event stream

The platform broadcasts every successfully decoded event to connected WebSocket
clients. Flow:

```
EVM logs → crawler → RabbitMQ → consumer (decode + upsert) → Redis Pub/Sub
                                                             ↘
                                           API server subscribes & forwards
                                                             ↘
                                           WebSocket clients (/api/v1/ws)
```

Redis is used as a thin fan-out bus between the consumer and the API because the
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
go run ./cmd/api
go run ./cmd/consumer
go run ./cmd/crawler
```
