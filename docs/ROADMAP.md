# Roadmap

Every stage has the same shape: **build** the obvious version, **break** it with the load script
and the flaky receiver, write an ADR, then fix it. The detailed spec for a stage is written in
`docs/stages/` when that stage is reached; this file is the map.

Stages 1–9 are the standard patterns. 10–15 add scale and coordination. 16–25 go deep on
concurrency at three layers: inside one process, across processes on one machine, and across
machines through the database.

## Part 1 — the standard patterns

| # | Build | Break | Lesson |
|---|---|---|---|
| 1 | Synchronous delivery: event endpoint calls subscribers inline | One endpoint 30s slow | Decouple ingest from delivery; what 202 means |
| 2 | Deliveries as Postgres rows, a worker polls them | Two workers double-send; kill a worker mid-delivery, row stuck forever | Concurrent consumers, `FOR UPDATE SKIP LOCKED`, leases and reclaiming abandoned work |
| 3 | Lease reclaim | Worker sends, then crashes before marking done | At-least-once; receiver idempotency; event id header; transactional outbox |
| 4 | Naive retry: fail, wait 1s, retry | Receiver down 60s with 1,000 pending | Exponential backoff + jitter, thundering herd, capped schedule, dead-letter |
| 5 | Shared worker pool | One slow subscriber; healthy ones' latency climbs | Head-of-line blocking, per-endpoint concurrency, bulkheads, circuit breaker / auto-disable |
| 6 | No ingest limits | Push faster than workers deliver | Back-pressure: where to apply it; queue depth as the key metric; graceful degradation |
| 7 | Add workers | Throughput stops improving | Find the real bottleneck (polling, connections); polling vs `LISTEN/NOTIFY`; honest broker evaluation |
| 8 | Logs, metrics, delivery-log endpoint | Re-run stage 5 blind, then with metrics | What you needed to see and couldn't |
| 9 | Signing (Standard Webhooks), secret rotation, SSRF guard, body caps | Point an endpoint at `localhost:5432` | What makes the service safe to point at |

## Part 2 — scale and coordination

| # | Build | Break | Lesson |
|---|---|---|---|
| 10 | Optional ordering key per event | One failed delivery must block everything behind it on that key | Partitions; ordering vs retries conflict; why ordering is opt-in |
| 11 | Multi-tenant ingest | Tenant A pushes 1M events; tenant B's 10 wait | Fair scheduling, weighted round-robin in the claim query, token-bucket limits, per-tenant quotas |
| 12 | Nothing; just run for hours | Tens of millions of rows; p99 climbs with no code change | Time partitioning, archival, index bloat, queue vs log; claim-check for large payloads (MinIO) |
| 13 | Move dispatch to Redis Streams or Kafka, Postgres stays system of record | Insert event, publish, crash in between | Dual-write problem; outbox done properly (relay or CDC/Debezium); consumer groups, offsets, rebalancing. **Decision point:** if Postgres is still fine at measured load, say so and stop here |
| 14 | Two API instances, two workers, Postgres replica | Lease reclaim and outbox relay run on two nodes at once | Leader election (advisory lock / etcd), split brain, fencing tokens |
| 15 | Toxiproxy between workers and both Postgres and the receiver | Latency, loss, full partitions under load | What CAP means here; timeouts vs retries at every hop; which failures the system handles vs lies about |

## Part 3 — concurrency, three layers deep

### Inside one worker process

| # | Build | Break | Lesson |
|---|---|---|---|
| 16 | asyncio worker | Plant a blocking call (sync DNS, HMAC over 5MB, sync DB call); p99 spikes for *every* endpoint | Blocking the loop; slow-callback debug mode; thread/process pools |
| 17 | `gather` over everything claimed | Claim 5,000 rows → 5,000 sockets, fd exhaustion, throttled | Semaphores; per-endpoint limits as second layer; httpx pool sizing |
| 18 | Plain worker loop | SIGTERM with 200 in flight: dropped, some already sent → resent later | Structured concurrency (TaskGroup), draining, drain timeouts, cancellation at every external await |
| 19 | In-process circuit breaker dict | Read, await, write — another task interleaves | asyncio still has races; critical sections; when a lock is right |

### Across processes on one machine

| # | Build | Break | Lesson |
|---|---|---|---|
| 20 | Signing in-process | Large payloads; one process pins a core, others idle | GIL; threads vs processes; process pool for CPU work; serialization overhead; free-threaded build |
| 21 | Per-process connection pool | 20 processes hit Postgres' connection limit; workers hang on checkout | Pools as shared resource; pgbouncer; checkout timeouts |

### Across machines

| # | Build | Break | Lesson |
|---|---|---|---|
| 22 | Claim query | 50 workers with/without `SKIP LOCKED`; then optimistic `UPDATE ... WHERE version = N` | Pessimistic vs optimistic concurrency; lock contention; idempotent state transitions in SQL |
| 23 | Lease expiry | Slow worker's lease expires, second worker claims, first finishes: both delivered | **Fencing tokens.** The most important ten lines of SQL in the project |
| 24 | Ordering keys (from 10) | "At most one in flight per key, across all workers" | Lock granularity: row lock vs striping vs advisory lock; hot keys |
| 25 | Timestamps everywhere | One worker's clock two minutes fast | Never compare wall clocks across machines; DB clock as authority; timeout vs deadline |

### Where Part 3 slots in

16–19 right after stage 5. 20–21 after stage 7. 22–25 alongside stage 14.
The receiver needs no changes; the load script will need to report open sockets and pool wait
time around stage 17.

## Deliberately out of scope

Multi-region, active-active replication, a real UI, transformations or a filtering DSL, any AI
feature. They don't teach anything the stages above don't, and they eat the time.
