# Idempotency, Versions, and Ordering

Idempotency is a core delivery concern, not an optional optimization. A
producer may retry after a timeout even though the broker accepted the event,
and a consumer may restart after performing an effect but before recording its
checkpoint. A robust system must make those repetitions safe.

## Three different problems

These mechanisms are related but not interchangeable:

1. **Deduplication** asks: have I already processed this operation?
2. **Version filtering** asks: is this state update older than the state I
   already applied?
3. **Ordering enforcement** asks: can I process this event yet, or am I still
   missing an earlier event?

The `idempotency_key` handles the first problem. A per-aggregate sequence or
version handles the other two.

## Pattern 1: idempotency key and deduplication

The producer assigns a stable key to the business operation and reuses it for
every retry:

```text
attempt 1: charge-card, id=msg-101, idempotency_key=payment-42
attempt 2: charge-card, id=msg-102, idempotency_key=payment-42
```

The broker or consumer stores the key and returns the previous result instead
of applying the operation twice.

This is appropriate for operations such as:

- charging a payment;
- creating an order;
- sending a notification;
- reserving inventory;
- issuing a refund.

The current broker implements a bounded in-memory version of this pattern.
Production recovery requires a durable deduplication store or a durable event
log; otherwise the deduplication history disappears after a restart.

## Pattern 2: discard stale versions

For state updates, each entity or aggregate can carry a monotonically
increasing version:

```text
Account A, version 10: balance = 90
Account A, version 11: balance = 70
Account A, version 10: balance = 90  ← stale, discard
```

The consumer stores the highest applied version and ignores events with a
lower or equal version.

Use this pattern when the event represents the latest desired state, such as:

- synchronizing a user profile;
- updating device configuration;
- replicating inventory state;
- maintaining a search index.

Do not use it for events whose individual effects matter. Discarding an old
`PaymentCaptured` or `EmailRequested` event would lose a business action, not
just an obsolete state update.

## Pattern 3: hold events until the expected version arrives

For workflows where every event matters, the consumer can hold later events
until the missing version arrives:

```text
received version 10 → process
received version 12 → hold
received version 11 → process 11, then release 12
```

This preserves ordering, but requires protection against an event that never
arrives. The hold queue therefore needs:

- a bounded size;
- a timeout;
- a retry or replay request;
- a dead-letter path;
- metrics for gaps and stalled aggregates.

This pattern is appropriate for ledgers, state machines, workflow transitions,
and any domain where applying version 12 before version 11 is unsafe.

## Choosing between the patterns

| Requirement | Recommended mechanism |
|---|---|
| Same command may be retried | Idempotency key |
| Only the newest state matters | Version filtering and stale-event discard |
| Every transition matters | Ordered hold-and-release |
| Consumer may restart | Durable checkpoint plus deduplication |
| Broker may fail over | Stable IDs, ACKs, replay, and deduplication |

## Recommended event metadata

An event should eventually carry enough metadata to make these decisions
explicit:

```text
message_id        unique transport message identity
idempotency_key   stable business operation identity
aggregate_id      entity or workflow being changed
aggregate_version monotonic version for that aggregate
created_at        producer creation time
```

Not every event needs every field. The producer and consumer contract should
state which fields are required for each topic.

## Current delivery semantics

The project currently targets at-least-once delivery while a TCP connection
remains healthy. The consumer explicitly acknowledges an event after processing
it. If the ACK is not received, the broker retries the same event up to three
times, with a two-second acknowledgement timeout. The TCP client keeps a
bounded set of recently acknowledged IDs so a retry crossing with an ACK is not
exposed to the application twice.

The project deliberately does not claim exactly-once business effects:

- Publish retries use the same idempotency key and are deduplicated by the
  broker while the key remains in its bounded memory.
- A consumer may observe a duplicate after a lost ACK, so processing must be
  idempotent even with the client-side duplicate filter.
- Events are volatile. Process crashes can lose cached, queued, or in-flight
  events before durable offsets exist.

There is an important failure boundary: if the connection disappears after the
broker removes an event from its in-memory subscription queue, that in-flight
event is not recovered from durable storage. A future WAL and consumer offsets
are required for recovery across process failure.

## Project direction

The next reliability steps are:

1. retain deduplication results durably;
2. add consumer checkpoints;
3. add aggregate versions to selected example topics;
4. demonstrate both stale-event discard and ordered hold-and-release;
5. expose deduplication hits, version gaps, held events, and expirations as
   metrics.
