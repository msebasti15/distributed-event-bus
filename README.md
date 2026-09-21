# Distributed Event Bus

An educational pub/sub event bus written from scratch in Go.

## Current scope

The first milestone implements the in-memory core:

- topic-based subscriptions;
- fan-out to independent consumers;
- bounded per-subscription queues;
- bounded per-topic cache for events published without active consumers;
- blocking and drop-on-full backpressure policies;
- per-subscription publish ordering;
- subscription and broker shutdown.

The network transport is TCP with a small length-prefixed binary protocol. Its
message payloads use fixed-width big-endian length prefixes rather than JSON,
keeping the hot path compact and avoiding serialization overhead. The broker
core does not depend on TCP, which leaves room for an optional gRPC adapter
later.

The event bus also exposes an HTTP control plane on port `9001`:

```bash
curl http://127.0.0.1:9001/state
curl -X POST 'http://127.0.0.1:9001/shutdown?redirect=127.0.0.1:19000'
```

Shutdown first stops new TCP connections and incoming publishes, drains active
subscriber queues, sends a `SHUTDOWN` notification, and then closes the client
connections. The redirect is optional.

The project will later add retries, idempotency, failure handling, a distributed transport, Prometheus/OpenTelemetry instrumentation, profiling, benchmarks, and load tests.

## Run tests

```bash
go test ./...
```

## Run locally

Start the broker:

```bash
go run ./cmd/event-bus -listen 127.0.0.1:19000
```

In another terminal, start a consumer:

```bash
go run ./cmd/consumer -address 127.0.0.1:19000 -standby 127.0.0.1:19001,127.0.0.1:19002 -topic orders -client-id consumer-a
```

Then publish events from a third terminal:

```bash
go run ./cmd/producer -address 127.0.0.1:19000 -standby 127.0.0.1:19001,127.0.0.1:19002 -topic orders -payload created -count 5 -interval 500ms
```

To demonstrate bounded queues and backpressure, run the delayed-consumer
scenario:

```bash
./scripts/run-delayed-consumers.sh
```

It starts the bus and producer first, waits five seconds, and then starts
three consumers. You can customize the scenario with `CONSUMER_DELAY_SECONDS`,
`MESSAGE_COUNT`, `QUEUE_SIZE`, and `CACHE_SIZE`.

To test cache replay directly, run:

```bash
./scripts/test-cache-replay.sh
```

To demonstrate broker migration with the connection manager:

```bash
./scripts/demo-connection-manager.sh
```

The controller requests a graceful shutdown of the primary after eight
seconds. The producer and consumer receive the redirect and reconnect to the
standby broker.

To simulate an abrupt primary crash without a `SHUTDOWN` event:

```bash
./scripts/demo-abrupt-failover.sh
```

The primary is terminated with `SIGKILL`; the connection manager must detect
the broken TCP connection and fail over to the standby.
