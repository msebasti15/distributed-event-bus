# Changelog

All notable changes to this project are documented here.

The project is pre-1.0. The current releases describe an educational TCP
pub/sub prototype; durable replication and recovery are not implemented yet.

## [Unreleased]

### Planned

- Add message IDs, acknowledgements, retry policies, and deduplication.
- Add Prometheus metrics, OpenTelemetry tracing, structured logs, and `pprof`.
- Add benchmarks and sustained load tests.
- Add a durable write-ahead log and consumer offsets.
- Add primary-to-standby replication and leader epochs.
- Add TLS, authentication, and authorization for the HTTP control plane.
- Add GitHub Actions for formatting, tests, race detection, and static checks.

## [0.1.2] - 2026-09-22

### Documentation

- Added the project roadmap.
- Added a complete changelog with current capabilities and limitations.
- Added ADRs for TCP transport, bounded queues, and client-side failover.
- Linked the evolution documentation from the README.

## [0.1.1] - 2026-09-22

### Documentation

- Added architecture notes describing intended use cases, trade-offs, current
  delivery guarantees, and the evolution path.
- Documented why the current standby behavior is client failover rather than
  broker replication.

## [0.1.0] - 2026-09-21

### Added

- In-memory topic-based pub/sub broker with fan-out.
- Bounded per-subscription queues.
- Blocking and drop-on-full backpressure policies.
- Bounded topic cache for events published without active consumers.
- Graceful broker shutdown with queue draining and optional redirect address.
- HTTP `/state` and `/shutdown` control endpoints.
- Length-prefixed binary TCP framing and message protocol.
- TCP producer and consumer clients.
- `ConnectionManager` with reconnect, standby failover, and subscription replay.
- Producer, consumer, and event-bus command-line executables.
- Local demonstration scripts for cache replay, graceful migration, and abrupt
  primary failure.
- Unit and TCP integration tests for the initial behavior.

### Known limitations

- Broker state is volatile and lost on process failure.
- Standby brokers do not replicate events from the primary.
- There are no message IDs, ACKs, durable offsets, or deduplication.
- The HTTP control plane is unauthenticated and intended for trusted networks.

[Unreleased]: https://github.com/msebasti15/distributed-event-bus/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/msebasti15/distributed-event-bus/releases/tag/v0.1.2
[0.1.1]: https://github.com/msebasti15/distributed-event-bus/releases/tag/v0.1.1
[0.1.0]: https://github.com/msebasti15/distributed-event-bus/releases/tag/v0.1.0
