# Graceful RabbitMQ consumers — completion note

**Date:** 2026-05-13  
**Source plan (authoritative copy on this machine):**  
`/Users/macbook/.cursor/plans/graceful_rabbitmq_consumers_44936877.plan.md`

## Summary

- Extracted **Basic.Cancel → drain → detached handler context** into `internal/infra/rabbitmq/graceful_consume.go` (`DetachedHandlerContext`, `GracefulConsumeParams`, `GracefulConsumeParamsDefaults`, `GracefulConsumeLoop`).
- Refactored **`(*rabbitmq.Consumer).Consume`** to call `GracefulConsumeLoop` (JSON malformed → Ack; handler error → Nack+requeue).
- **`EventURIConsumer.RunChain`** and **`ServiceURIConsumer.RunChain`** now use non-empty consumer tags and `GracefulConsumeLoop` (event_uri preserves ack/nack/advance cursor semantics; service_uri remains always-ack).
- **Docker Compose:** `stop_grace_period: 3m` on `consumer`, `trustrank-worker`, `uri-resolver`.

## Idle-drain note (from plan self-review)

If after `Basic.Cancel` the `deliveries` channel stays open with no messages until `DrainDeadline`, shutdown can wait up to the drain timeout (default 2m). Further **idle detection** is out of scope for this change.

## Implementation checklist

- [x] Task 1: `graceful_consume.go` + `graceful_consume_test.go` (DetachedHandlerContext TDD)
- [x] Task 2: `Consumer.Consume` uses `GracefulConsumeLoop`
- [x] Task 3: `consumer_event_uri.go` — tag + graceful loop + infra nack/requeue
- [x] Task 4: `consumer_service_uri.go` — tag + graceful loop + always ack
- [x] Task 5: `docker-compose.yaml` — `stop_grace_period: 3m`
- [x] Task 6: This file

## Verification commands

```bash
cd erc-8004-benchmarking-be
go test ./internal/infra/rabbitmq/...
go test ./internal/app/uribootstrap/... ./internal/app/trustrank/...
docker compose config
```
