# Performance Principles

Performance is a design constraint of this project. It is not a final
optimization pass.

## What we measure

Changes that touch publish, routing, delivery, framing, reconnect, or queueing
should be evaluated with:

- throughput in messages per second;
- end-to-end latency at p50, p95, and p99;
- allocations per message;
- bytes allocated per message;
- CPU and memory under sustained load;
- behavior when one consumer is slow;
- reconnect and failover recovery time.

The benchmark environment and message sizes must be documented with the
results. A faster median with worse p99 latency or unbounded memory is not an
improvement.

## Hot-path principles

- Keep the wire protocol binary and length-prefixed.
- Avoid JSON, reflection, and unnecessary serialization in the event path.
- Avoid copying payloads unless ownership or safety requires it.
- Keep queues bounded and make backpressure explicit.
- Minimize lock scope around topic and subscription metadata.
- Do not hold global broker locks while blocking on a consumer.
- Reuse buffers only when profiling demonstrates allocation pressure and the
  ownership rules remain clear.
- Prefer one long-lived goroutine per connection/subscription over per-event
  goroutines.
- Make retry and reconnect backoff bounded so a failure does not create a busy
  loop.

## Current known costs

The current implementation intentionally favors clarity while keeping the
main path bounded:

- each subscription has a channel-backed queue;
- each TCP connection serializes writes with a mutex;
- publish acknowledgements require a pending-message lookup;
- deduplication uses bounded maps;
- graceful drain currently polls queue length and should be benchmarked before
  being replaced with a condition-based design;
- the Connection Manager retries periodically and should be measured under
  large endpoint failure storms.

These are explicit candidates for profiling, not assumptions that they are
already optimal.

## Performance gates

Before calling a milestone complete:

1. Add or update a focused benchmark.
2. Run the benchmark against the previous implementation.
3. Run the race detector for concurrent changes.
4. Check allocation and memory behavior.
5. Document any intentional trade-off in an ADR.

Correctness and bounded resource usage take priority over a benchmark win that
weakens delivery guarantees.
