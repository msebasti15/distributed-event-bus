# ADR 0004: Treat performance as a design constraint

- Status: Accepted
- Date: 2026-09-23

## Context

The event bus is intended to demonstrate systems engineering. Throughput,
latency, allocations, memory bounds, and slow-consumer behavior are part of
the design rather than an afterthought.

## Decision

Every feature that touches the event path must consider performance and, when
appropriate, include a benchmark and resource-bound analysis. We prefer a
small, measurable implementation over speculative optimization.

## Consequences

The project keeps a binary protocol, bounded queues, explicit backpressure,
and narrow lock scopes. Profiling and benchmark results become part of the
project documentation. Correctness, delivery semantics, and bounded resource
usage remain higher priorities than maximum raw throughput.
