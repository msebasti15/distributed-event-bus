# ADR 0003: Keep failover policy in the Connection Manager

- Status: Accepted
- Date: 2026-09-22

## Context

Clients need to survive abrupt TCP failures and planned broker migration. The
broker core should not depend on a particular client discovery mechanism.

## Decision

Put endpoint selection, reconnect backoff, standby failover, redirect handling,
and subscription replay in a client-side `ConnectionManager`.

## Consequences

The broker core remains transport- and topology-focused. Producers and
consumers can recover connections independently. This does not replicate
broker state; reliable event recovery still requires message IDs, ACKs,
offsets, persistence, and replication.
