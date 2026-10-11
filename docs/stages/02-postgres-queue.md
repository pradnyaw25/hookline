# Stage 2 — deliveries as Postgres rows, a worker polls them

**Build the obvious thing.** Stage 1 ended with a 202 that promised delivery and an in-process
task that could not keep the promise across a restart. Make the promise durable: write each
delivery as a row, and have a separate worker process pick rows up and send them.

## Build

Postgres via `make db` (Docker) or `make db-local` (Homebrew `postgresql@17`, data in
`.pgdata/`). Schema lives in numbered SQL files under `src/hookline/migrations/`, applied by
`make db-migrate`. Raw SQL through `psycopg`; the claim query is the lesson, so no ORM hides it.

Tables: `apps`, `endpoints`, `events`, `deliveries`. One delivery row per event × endpoint,
created in the same transaction as the event. A delivery has a `status`:
`pending → delivering → delivered | failed`, plus `attempts`, `last_status`, `last_error`.

API changes:

| Method | Path | Returns |
|---|---|---|
| `POST /apps/{app_id}/events` | unchanged contract | 202 once the event and its delivery rows are committed |
| `GET /apps/{app_id}/events/{event_id}` | the event with its delivery rows from the database |

Worker, `make worker` (`python -m hookline.worker`), the obvious loop:

```
loop:
  rows = SELECT ... FROM deliveries WHERE status = 'pending' ORDER BY created_at LIMIT batch
  if none: sleep poll_interval; continue
  UPDATE those rows SET status = 'delivering'
  for each: POST to the endpoint; UPDATE status = 'delivered' or 'failed'
```

No locking, no lease, no retry (retry is stage 4). The ingest API never talks to a subscriber.

## Break it

Two breaks, both in the roadmap row for this stage.

**1. Two workers double-send.**

```bash
make receiver
make api
make worker            # terminal 3
make worker            # terminal 4
curl -X PUT localhost:9000/control/load-0 -H 'content-type: application/json' -d '{"delay_ms": 100}'
make load ARGS="--events 200 --concurrency 20"
```

Read the `duplicate deliveries` column. Then read the two workers' logs for the same delivery id.

**2. Kill a worker mid-delivery; the row is stuck forever.**

```bash
curl -X PUT localhost:9000/control/load-0 -H 'content-type: application/json' -d '{"delay_ms": 30000}'
make load ARGS="--events 20 --concurrency 20 --wait 120"     # in another terminal, after ~2s:
kill -9 <pid of one worker>
```

Then `SELECT status, count(*) FROM deliveries GROUP BY 1`. The load script will time out;
the `delivering` rows never move again, however long you wait, and the surviving worker
will not touch them.

Questions to answer from the output, not from intuition:

- In break 1, how many events were delivered twice, and what did each worker believe about
  them? Where exactly is the window between "I read it" and "I own it"?
- In break 2, what does the surviving worker see when it polls? Why is `delivering` not
  enough information to decide whether the row is abandoned?
- Does the stage-1 restart test now pass? Accept 200 events with no worker running, start
  the API again, then start a worker. Count what arrives.

## ADR

ADR-0002: *how concurrent workers claim work without overlapping, and how abandoned work is
recovered.* The claim query, the lease, and who reclaims.

## Done when

- Two workers, `make load ARGS="--events 200 --concurrency 20"` reports 200/200 delivered
  with `0 duplicate deliveries`.
- `kill -9` one of two workers while it holds deliveries against a stalled receiver. Every
  delivery completes within one lease interval of the kill, with no row left in `delivering`.
- Stop and restart the API and the worker with events accepted but undelivered. Nothing is lost.
- ADR-0002 exists, and the stage-1 promise sentences are now true across a restart.

Next: stage 3, the worker sends and then crashes before marking done.
