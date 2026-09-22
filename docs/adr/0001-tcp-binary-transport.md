# ADR 0001: Use TCP with a binary application protocol

- Status: Accepted
- Date: 2026-09-21

## Context

The project needs a transport that exposes real networking concerns while
remaining small enough to implement and inspect from scratch.

## Decision

Use TCP as the transport and define a length-prefixed binary application
protocol above it. TCP provides reliable ordered bytes, while the protocol
defines message boundaries, message types, limits, and lifecycle frames.

## Consequences

This makes partial reads, connection failures, ordering, and backpressure
explicit. TCP also has head-of-line behavior and does not provide message
semantics by itself. QUIC and gRPC remain possible future adapters.
