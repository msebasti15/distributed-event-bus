# Distributed Event Bus

An educational pub/sub event bus written from scratch in Go.

See [Architecture Notes](docs/architecture.md) for the intended use cases,
trade-offs, delivery guarantees, and evolution path.

Project evolution is tracked in the [roadmap](docs/roadmap.md),
[changelog](CHANGELOG.md), and [architecture decision records](docs/adr/).
Performance expectations and measurement rules are documented in
[Performance Principles](docs/performance.md).
Idempotency and ordering strategies are described in
[Idempotency, Versions, and Ordering](docs/idempotency.md).

## Current scope

The first milestone implements the in-memory core:

- topic-based subscriptions;
- fan-out to independent consumers;
- bounded per-subscription queues;
- message IDs, broker acknowledgements, and bounded deduplication;
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

Run the current microbenchmarks with allocations:

```bash
go test ./... -run '^$' -bench . -benchmem
```

Versioned benchmark results and the recording workflow are documented in
[benchmarks/README.md](benchmarks/README.md).

To demonstrate publish acknowledgements and duplicate detection:

```bash
./scripts/demo-ack-dedup.sh
```

To demonstrate consumer delivery acknowledgements and redelivery:

```bash
./scripts/demo-delivery-ack.sh
```

The consumer intentionally waits longer than the ACK timeout. The broker
redelivers the same event, after which the consumer acknowledges it.

## Demonstrations

Every demonstration prints its purpose, topology, trigger, expected result,
and known limitation before opening terminals.

| Script | Scenario | Expected observation |
|---|---|---|
| `run-local.sh` | Basic fan-out | Every consumer receives each event. |
| `run-delayed-consumers.sh` | Bounded queues and delayed consumers | Events are replayed from the in-memory cache. |
| `test-cache-replay.sh` | Cache without active consumers | A later consumer receives cached events. |
| `demo-ack-dedup.sh` | Producer/broker ACK | First publish is accepted; the repeat is duplicate. |
| `demo-delivery-ack.sh` | Consumer delivery ACK | Missing ACK causes redelivery; consumer ACKs the broker. |
| `demo-connection-manager.sh` | Graceful migration | Clients follow the broker redirect and resubscribe. |
| `demo-abrupt-failover.sh` | Abrupt failure | Clients reconnect to the standby after SIGKILL. |

The demos use separate localhost ports so they can be run independently. If a
previous run is still active, stop its terminals or override the addresses with
the corresponding environment variables.

## Run locally

Start the broker:

```bash
go run ./cmd/event-bus -listen 127.0.0.1:19000 -log-level debug
```

The broker supports `error`, `warn`, `info` (default), and `debug` logging
levels. Use `debug` during demonstrations to see connections, subscriptions,
publish ACKs, deliveries, delivery retries, and consumer ACKs.

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
