# erc-8004-benchmarking-be

Go backend for the ERC-8004 agent benchmarking platform. Runtime binaries are
organized under `cmd/workers/*`:

- `cmd/workers/indexer` — EVM log crawler that publishes raw logs to RabbitMQ.
- `cmd/workers/event-decoder` — RabbitMQ consumer that decodes raw logs to typed events, upserts MongoDB, and publishes Redis notifications.
- `cmd/workers/uri-bootstrap` — Resolves agentURI / feedbackURI (IPFS, HTTP, data:) into off-chain data.
- `cmd/workers/trustrank` — Processes identity events; rule-classifies decoded feedback.
- `cmd/workers/feedback-others` — Consumes the rule-undecided (`others`) residual and resolves its category with the AI service.
- `cmd/workers/feedback-grader` — Consumes classified feedback, runs the validation gate, computes quality scores, and applies live incremental updates.
- `cmd/workers/score-refresh` — Replays feedback history on a cron to recompute authoritative reputation, composite breakdown, and WalletTrust.
- `cmd/workers/rescale` — Retroactively corrects scores when a tag's value scale changes.
- `cmd/workers/api` — Serves the REST API and the WebSocket hub.
- `cmd/workers/wallet-enrich` — Enriches wallets with external on-chain signals (balance, age, counterparties, ENS).
- `cmd/workers/oasf-schema-refresh` — Refreshes the OASF skill and domain taxonomy.

## Architecture

### Ingestion

![Architecture Ingestion](assets/architecture_ingestion_drawio.png)

### Scoring

![Architecture Scoring](assets/architecture_scoring_drawio.png)

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
