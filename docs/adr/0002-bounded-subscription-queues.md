# ADR 0002: Give every subscription a bounded queue

- Status: Accepted
- Date: 2026-09-21

## Context

A slow consumer must not cause unbounded memory growth or block unrelated
consumers indefinitely.

## Decision

Every subscription owns a bounded queue. The broker supports blocking the
publisher or rejecting delivery when the queue is full.

## Consequences

The system has explicit backpressure and isolation between consumers. The
trade-off is that callers must choose whether blocking or dropping is correct
for their workload. A durable queue is outside the current in-memory scope.
