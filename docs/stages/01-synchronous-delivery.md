# Stage 1 — synchronous delivery

**Build the obvious thing.** The event endpoint calls every subscriber inline and returns when
they have all answered. No queue, no retries, no database.

## Build

Everything in `src/hookline/`. In-memory storage (dicts) is fine; it goes away at stage 2.

API contract (the load script depends on it):

| Method | Path | Body | Returns |
|---|---|---|---|
| `POST` | `/apps` | `{"name": str}` | `{"id": str, "name": str}` |
| `POST` | `/apps/{app_id}/endpoints` | `{"url": str}` | `{"id": str, "url": str}` |
| `POST` | `/apps/{app_id}/events` | `{"type": str, "payload": object}` | `{"id": str, ...}` |

For stage 1, `POST .../events` POSTs the payload as JSON to every endpoint of the app with
`httpx`, waits for all of them, then returns 200 with per-endpoint results. Use `str(uuid4())`
for ids. Send a `Webhook-Id: <event id>` header; the receiver keys its attempt and duplicate
counters on it.

## Break it

```bash
make receiver
make api
make load ARGS="--events 50 --concurrency 10 --endpoints 2"      # baseline: note the numbers
curl -X PUT localhost:9000/control/load-1 -H 'content-type: application/json' -d '{"delay_ms": 30000}'
make load ARGS="--events 50 --concurrency 10 --endpoints 2 --timeout 5 --wait 90"
```

Questions to answer from the output, not from intuition:

- What happened to ingest latency for the *healthy* endpoint `load-0`?
- What status did the producer get, and did `load-0` still receive the events? Is the producer's
  view of "delivered" true?
- Set `{"down": true}` instead. What does the producer see now? Should it?
- What is the producer actually asking the service when it POSTs an event: "deliver this" or
  "accept responsibility for this"?

## ADR

ADR-0001: *why ingest and delivery are decoupled, and what the ingest response means.* Decide
the HTTP status `POST /events` returns from now on and what it promises.

## Done when

- `make load ARGS="--events 50 --endpoints 2"` reports 100/100 delivered with the receiver healthy.
- You can state, in one sentence each, what the service promises the producer and what it
  promises the subscriber. These sentences will change; write them down anyway.
- ADR-0001 exists.

Next: stage 2, a queue in Postgres.
