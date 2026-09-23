# Architecture Notes

## What this project is

This project is a small TCP-based publish/subscribe event bus implemented from
scratch in Go. It is designed to make delivery semantics, backpressure,
connection failure, graceful migration, and client-side failover explicit.

The current implementation is an in-memory broker with a binary TCP protocol.
The standby broker mechanism provides client failover, but it does not yet
replicate broker state or provide durable recovery after a process crash.

## System shape

```text
Producer ───────┐
                ▼
        Connection Manager
                │ TCP
                ▼
        ┌───────────────┐
        │ Primary Broker│──── optional future replication ────► Standby
        └───────┬───────┘
                │ fan-out
        ┌───────┴────────┐
        │ bounded queues │
        └───┬────────┬───┘
            ▼        ▼
        Consumer A  Consumer B
```

The broker core is independent from TCP. This allows other transports, such
as gRPC, to be added without changing routing and queueing behavior.

## When this architecture is a good fit

Choose this design when the system needs:

- low-latency event delivery inside a controlled network;
- a small number of well-defined producer and consumer services;
- topic-based fan-out rather than a large durable event log;
- explicit backpressure and isolation for slow consumers;
- control over the wire protocol and client reconnect behavior;
- an operationally simple broker that can be deployed close to its clients;
- an educational or embedded event bus where the delivery semantics are part
  of the product.

Typical examples include internal service notifications, workflow signals,
device or edge gateways, test infrastructure, and small-to-medium event-driven
applications where the event volume and retention requirements are bounded.

## When to choose something else

This architecture should not be the default choice when the main requirement
is:

- a large, durable, replayable event history;
- multi-region replication and high availability out of the box;
- very large consumer groups and horizontal partitioning;
- browser-native clients, where WebSocket or HTTP APIs may be more suitable;
- broad language interoperability with generated contracts, where gRPC may be
  preferable;
- exactly-once business effects across external systems;
- a managed operational model where Kafka, NATS, RabbitMQ, or a cloud service
  already solves the problem well.

The project deliberately exposes these trade-offs instead of hiding them
behind a generic messaging abstraction.

## Main design decisions

### TCP as the transport

TCP provides reliable, ordered bytes, but not messages. The protocol therefore
adds explicit length-prefixed binary framing. This keeps the hot path compact
and makes partial reads, connection loss, and framing errors visible.

TCP is a good first transport here because it forces the implementation to
handle backpressure and failure boundaries. QUIC could be evaluated later for
multiplexing and faster connection recovery.

### Bounded queues

Every subscription owns a bounded queue. A slow consumer must not consume
unlimited memory or silently block unrelated consumers.

The broker supports two initial policies:

- `Block`: apply backpressure to the publisher;
- `Drop`: reject delivery when a queue is full.

Production deployments should select this policy based on whether losing an
event is acceptable.

### In-memory cache

Events published while a topic has no active subscribers can be retained in a
bounded in-memory cache. The cache keeps recent events and is lost when the
broker stops. It is not a durable log and must not be described as one.

### Connection Manager

Clients receive an ordered list of broker endpoints. The Connection Manager:

- connects to the preferred broker;
- reconnects after abrupt TCP failures;
- tries standby brokers;
- reapplies consumer subscriptions;
- honors a redirect received during graceful migration;
- prefers the original primary again after an unplanned failure.

This is client-side failover, not broker replication. Reliable failover of
business events additionally requires message IDs, acknowledgements,
deduplication, offsets, and durable broker state.

### Graceful migration

The HTTP control plane can stop a broker with an optional replacement address.
The broker stops accepting new input, drains active subscription queues, sends
a `SHUTDOWN` frame, and closes the connection. Clients then reconnect to the
replacement broker and consumers re-subscribe.

This is useful for planned maintenance and capacity migration. It is separate
from recovery after a crash, where no shutdown frame is available.

## Current delivery guarantees

The current implementation should be treated as:

- ordered delivery per subscription while connected;
- fan-out to active subscriptions;
- bounded memory per subscription;
- at-most-once behavior across an abrupt connection failure unless the client
  adds its own retry and deduplication strategy;
- volatile cache semantics;
- no durable recovery after broker process loss.

The target production-oriented model is likely to be **at-least-once delivery
with idempotent consumers**, rather than promising exactly-once effects.

Each event can carry both a `message_id` and an `idempotency_key`. The former
identifies a message and correlates its ACK; the latter identifies the business
operation that must not be applied more than once. Retries must preserve the
same idempotency key.

## Evolution path

The architecture can evolve in stages:

1. Add message IDs, ACKs, retries, and deduplication.
2. Add Prometheus metrics, tracing, structured logs, and `pprof`.
3. Add a durable write-ahead log and consumer offsets.
4. Add primary-to-standby replication and an epoch/term to prevent split-brain.
5. Add load tests, benchmarks, TLS, authentication, and authorization.
6. Evaluate gRPC or QUIC as optional transports over the same broker core.
