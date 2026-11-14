# Roadmap

The short course: six stages, one piece of infrastructure (Postgres), everything written by
hand. The original 25-stage course is kept in [ROADMAP-full.md](ROADMAP-full.md) for later.

Every stage has the same shape: **build** the obvious version, **break** it with the load
script and the flaky receiver, write an ADR, then fix it. The detailed spec for a stage is
written in `docs/stages/` when that stage is reached; this file is the map.

You can stop after any stage and still have a working service.

| # | Build | Break | Lesson |
|---|---|---|---|
| 1 | Synchronous delivery: event endpoint calls subscribers inline | One endpoint 30s slow | Decouple ingest from delivery; what 202 means |
| 2 | Deliveries as Postgres rows, a worker polls them | Two workers double-send; kill a worker mid-delivery, row stuck forever | Concurrent consumers, `FOR UPDATE SKIP LOCKED`, leases; reclaiming means re-sending, so delivery is at-least-once |
| 3 | Naive retry: fail, wait 1s, retry | Receiver down 60s with 200 pending; count the attempts | A `next_attempt_at` column as the delay queue, exponential backoff + jitter, attempt cap, dead-letter |
| 4 | Live delivery feed over **SSE**: each connected client polls the attempts table once a second | 30 feeds open during a load run: queries per second, pool wait, feed lag | Polling vs push; `LISTEN/NOTIFY` as the doorbell and the table as the log; resuming with `Last-Event-ID` because a notification can be missed |
| 5 | Dashboard over **WebSocket**: one HTML page that filters the feed, pauses an endpoint, replays a dead-lettered delivery | A client that stops reading while the load script runs | Back-pressure on push connections; bounded per-client queues and a drop policy; ping/pong and dead connections; SSE vs WebSocket |
| 6 | Nothing new; watch the dashboard | One endpoint stalls; healthy endpoints' latency climbs | Head-of-line blocking; per-endpoint concurrency limits in the claim query; a circuit breaker that every worker agrees on |

## Why each piece is here

Each technology has to solve a problem the previous stage actually has. If a stage feels like
it is using one for its own sake, that is worth an ADR.

- **Postgres** arrives at stage 2 because a 202 is a promise, and a promise has to survive a
  restart. The same table then serves as the queue (2), the retry schedule (3), the log the
  feed reads from (4), the place "paused" and "dead" live so the dashboard and the worker see
  the same thing (5), and the counters several workers must agree on (6). One store, one
  source of truth, transactions between all of it.
- **SSE** arrives at stage 4 because the feed is one-way and the browser's `EventSource`
  reconnects and resumes by itself. Write the `text/event-stream` response by hand; it is
  about fifteen lines and the framing (`id:`, `event:`, `data:`, blank line) is the lesson.
- **WebSocket** arrives at stage 5 because the browser starts talking back. If a feature only
  needs server-to-browser, it stays on SSE; deciding which is which is part of the stage.

## Why not Redis

Considered, and left out on purpose. Everything Redis would do here, Postgres already does
with data the service has to keep anyway: the queue is a table, the delay queue is a
timestamp column, and `LISTEN/NOTIFY` is the pub/sub. A second store would mean two places
that must agree, which is the dual-write problem from stage 13 of the full course.

The signals that would change this: `NOTIFY` volume showing up in the database's own
latency, several API instances each holding thousands of feed connections, or a rate limiter
hot enough that a row per endpoint contends. Measure first; that is stage 13's whole point.

## What changed from the full course

- Stages 1, 2, 4 and 5 of the full course are here as stages 1, 2, 3 and 6. Stage 3's
  at-least-once lesson is folded into stage 2.
- A small UI is in scope. The full course excluded one; here it is the reason for stages 4
  and 5, and the instrument for stage 6.
- Back-pressure at ingest, signing, multi-tenancy, ordering, partitioning, leader election and
  the deep concurrency stages are deferred, not dropped.

## Still out of scope

Multi-region, a frontend framework or build step (one static HTML file is enough), auth on the
dashboard, any AI feature.
