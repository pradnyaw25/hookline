# ADR-0002: Workers claim deliveries with `FOR UPDATE SKIP LOCKED` and hold them under a lease

**Stage:** 2  **Date:** 2026-10-11  **Status:** accepted

## What broke

Stage 2's obvious worker: `SELECT id ... WHERE status = 'pending' LIMIT 10`, then
`UPDATE ... SET status = 'delivering'`, then POST, then `UPDATE ... SET status = 'delivered'`.
Each statement in its own transaction.

**Break 1: two workers double-send.**

```bash
make worker & make worker &
curl -X PUT localhost:9000/control/load-0 -H 'content-type: application/json' -d '{"delay_ms": 100}'
make load ARGS="--events 200 --concurrency 20"
```

| | one worker (stage 1 baseline) | two workers |
|---|---|---|
| delivered | 200/200 | 200/200 |
| attempts at receiver | 200 | **360** |
| duplicate deliveries | 0 | **160** |
| `sum(attempts)` in `deliveries` | 200 | 360 |
| delivery ids claimed by both workers (from logs) | — | 160 of 200 |

The two workers' logs for delivery 1, one second apart in wall clock, identical otherwise:

```
15:06:34 210-57433 claimed 10: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
15:06:34 210-57435 claimed 10: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
15:06:34 210-57433 delivery=1 event=c5084848-… delivered status=200 120ms
15:06:34 210-57435 delivery=1 event=c5084848-… delivered status=200 120ms
```

The window is between the `SELECT` and the `UPDATE`: both workers read the same ten pending
ids, both set them to `delivering`, both believe they own them. The second `UPDATE` is a
no-op that nobody checks. Nothing in the database ever said "this row is mine"; `status =
'delivering'` says only "someone, at some point, started".

**Break 2: kill a worker mid-delivery; its rows are stuck forever.**

```bash
make worker                                  # exactly one
curl -X PUT localhost:9000/control/load-0 -H 'content-type: application/json' -d '{"delay_ms": 30000}'
make load ARGS="--events 20 --concurrency 20 --wait 75"
kill -9 <worker>                             # 3 s in, while it holds 10 rows against the stall
make worker                                  # a fresh one
```

| | at kill (t+3 s) | after the fresh worker ran for 30 s |
|---|---|---|
| `pending` | 10 | 0 |
| `delivering` | 10 | **10, `updated_at` unchanged since the kill** |
| `delivered` | 0 | 10 |
| receiver `unique_events_succeeded` | 0 | **20** |

The fresh worker polled for `pending`, found the ten rows the dead worker never touched,
delivered them, and then saw nothing: the ten `delivering` rows are invisible to it, and
stay that way however long you wait. They are also invisible to any future retry, dead-letter
or metric. `delivering` cannot tell "in flight" from "abandoned", because it carries no
owner and no deadline.

The receiver's side is worth a second look. The load script reported 20/20 delivered. The
receiver counted the dead worker's ten POSTs as successes, because it was sleeping through
the 30 s stall when the client socket closed and recorded a 200 anyway. So the subscriber
has the events, the database says nobody delivered them, and whoever eventually reclaims
those rows will deliver them again. That is not a bug to fix here; it is stage 3.

The same run also answers stage 1's open question. Twenty events were accepted; the only
worker died; nothing was lost. The ten rows it had not reached were delivered by its
replacement, because they were rows, not tasks in a dead process.

Both bugs interact. In the first attempt at break 2, run with two workers, the surviving
worker had *also* claimed the dead worker's rows (break 1), so it "recovered" them by
accident, 30 s later, with 10 duplicates. Double-claiming masked the stuck rows. The clean
reproduction above uses one worker, then a fresh one.

## Options

1. **Advisory lock around the poll.** `pg_advisory_xact_lock` so only one worker claims at a
   time. Fixes break 1 for the cost of serialising every claim. Does nothing for break 2.
2. **Claim with one atomic statement.** `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP
   LOCKED) RETURNING`. Each row is locked by the worker that selects it, locked rows are
   skipped rather than waited on, and the status change rides in the same statement. No two
   workers can claim the same row; workers never block each other. Does nothing for break 2
   on its own.
3. **A lease on every claim.** Add `locked_by` and `locked_until`. Claiming sets both; the
   claim query treats `delivering` rows whose lease has expired exactly like `pending` ones.
   Abandoned work recovers itself after one lease interval, with no separate reaper and no
   coordinator. Costs: the lease must be longer than the slowest legitimate delivery, or a
   live worker's row gets reclaimed under it (stage 23); and a reclaimed row *will* be sent
   again, so delivery becomes at-least-once (stage 3).
4. **Heartbeats.** Workers register themselves and ping a table; a reaper releases rows held
   by workers who stopped pinging. Recovers faster than a fixed lease, but adds a second
   table, a second loop, and a new way to be wrong about liveness. Not yet.

## Decision

Options 2 and 3 together: the claim query is

```sql
UPDATE deliveries SET status = 'delivering', attempts = attempts + 1,
       locked_by = %(worker)s, locked_until = now() + %(lease)s, updated_at = now()
 WHERE id IN (
     SELECT id FROM deliveries
      WHERE status = 'pending' OR (status = 'delivering' AND locked_until < now())
      ORDER BY id LIMIT %(batch)s
      FOR UPDATE SKIP LOCKED)
RETURNING id
```

The one reason: it is the only option that fixes both breaks in one statement, and every
later stage (retries, fairness, ordering, fencing) is a change to this `WHERE` clause rather
than a new mechanism.

Lease length is `WORKER_LEASE_S`, default 90 s, which must stay above `DELIVERY_TIMEOUT_S`
(60 s) plus margin. A delivery that outlives its lease is the stage 23 failure.

## Consequences

- Two workers no longer overlap, and the claim query is the single place where concurrency
  between workers is decided. The re-runs, same commands as above, lease 45 s for the kill:

  | | obvious worker | SKIP LOCKED + lease |
  |---|---|---|
  | break 1: attempts / duplicates | 360 / 160 | 200 / 0 |
  | break 1: rows per worker | overlapping | 100 / 100 |
  | break 2: rows stuck in `delivering` | 10, forever | 0 |
  | break 2: reclaimed by the survivor | never | 43 s after the kill (lease 45 s) |
  | break 2: `attempts` on reclaimed rows | 1 | 2 |
  | break 2: receiver duplicates | 0 (it counted the dead worker's POSTs) | 10 |
  | restart: 100 accepted with no worker, API bounced, worker started | — | 100/100, 0 duplicates |
- An abandoned row is recovered within one lease interval, by whichever worker polls next.
  Nothing recovers it sooner; a 90 s lease means up to 90 s of silence for that event.
- Delivery is now at-least-once, and the receiver has already shown the duplicate: it got the
  dead worker's POSTs *and* will get the reclaimed ones. `attempts` records it; nothing
  prevents it. Stage 3.
- The `pending` partial index no longer serves the claim; the index is now on
  `(status, locked_until, id)` so expired leases are found without a scan. Watch this index
  at stage 12.
- Two workers, one row, both legitimately holding it in sequence after a lease expiry, can
  both write `delivered`. The second write is wrong and nothing stops it. Stage 23.
