# Roadmap

This roadmap describes the intended evolution of the project. It is ordered
by dependency: correctness and observability come before distributed recovery.

## Done

### v0.1 — Initial prototype

- In-memory broker and topic fan-out.
- Bounded queues and backpressure.
- Binary TCP transport.
- Producer and consumer executables.
- Cache replay for topics without active consumers.
- HTTP state and shutdown control plane.
- Graceful migration and client-side failover demonstrations.

## Next

### v0.2 — Delivery reliability

- Introduce a unique `message_id` on every event.
- Add publish and delivery acknowledgements.
- Define retry behavior and failure boundaries.
- Add deduplication at the broker and consumer boundaries.
- Document the exact at-least-once/at-most-once semantics.

### v0.3 — Observability and performance

- Add Prometheus counters, gauges, and histograms.
- Add OpenTelemetry spans around publish, routing, delivery, and reconnect.
- Add structured logs and correlation IDs.
- Add `pprof`, microbenchmarks, and sustained load tests.
- Publish baseline throughput and latency results.

## Later

### v0.4 — Durable single-node broker

- Add a write-ahead log.
- Persist topic state and consumer offsets.
- Recover unprocessed events after restart.
- Add retention and compaction policies.

### v0.5 — Replicated brokers

- Replicate events from primary to standby.
- Define synchronous versus asynchronous replication.
- Add leader epochs and fencing to prevent split-brain.
- Promote a standby with a clear recovery protocol.

### v0.6 — Production hardening

- TLS and client authentication.
- Authorization by topic and operation.
- Authenticated HTTP administration.
- Configuration files and container images.
- Resource limits, rate limiting, and operational runbooks.

## Explicit non-goals for the first major version

- Claiming exactly-once business effects.
- Replacing Kafka or another durable event-log platform at large scale.
- Supporting arbitrary browser clients directly over the binary TCP protocol.
- Hiding delivery trade-offs behind an overly broad abstraction.
