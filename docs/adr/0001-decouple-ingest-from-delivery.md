# ADR-0001: Ingest and delivery are decoupled; `POST /events` returns 202 and promises delivery, not a result

**Stage:** 1  **Date:** 2026-11-25  **Status:** accepted

## What broke

Stage 1 delivers inline: `POST /apps/{id}/events` starts one delivery per endpoint, waits for
every endpoint to answer, then returns 200 with the list of results. Two endpoints: `load-0`
healthy, `load-1` made slow, then made dead.

```bash
make load ARGS="--events 50 --concurrency 10 --endpoints 2"                           # baseline
curl -X PUT localhost:9000/control/load-1 -H 'content-type: application/json' -d '{"delay_ms": 30000}'
make load ARGS="--events 50 --concurrency 10 --endpoints 2 --timeout 5 --wait 90"     # slow
curl -X PUT localhost:9000/control/load-1 -H 'content-type: application/json' -d '{"down": true}'
make load ARGS="--events 50 --concurrency 10 --endpoints 2 --timeout 5 --wait 10"     # dead
```

| | baseline | `load-1` stalls 30 s | `load-1` answers 503 |
|---|---|---|---|
| producer status | 200 ×50 | **ReadTimeout ×50** | 200 ×50 |
| ingest latency p50 | 0.038 s | 5.012 s | 0.036 s |
| ingest throughput | 248 events/s | 2.0 events/s | 251 events/s |
| delivered (receiver) | 100/100 | **100/100** | **50/100** |
| `load-0` latency p50 | 0.030 s | 0.061 s | 0.029 s |
| `load-1` latency p50 | 0.030 s | 30.06 s | never |
| max in flight at `load-1` | 3 | 50 | 2 |
| retried by anyone | — | — | 0 |

**Slow subscriber.** Every producer call timed out at the producer's own 5 s limit, because
the response waits for the slowest endpoint. The healthy endpoint was unaffected: deliveries
start together, so `load-0` had its events in 61 ms. The service did not notice the producer
leave; it finished all 50 deliveries to `load-1` thirty seconds later and built 50 result
lists that nobody read. The producer's limit of 10 calls in flight stopped protecting the
service: each time the producer gave up and moved on, the service still held the old
request, and 50 were open against `load-1` at once.

The producer's view and the truth point in opposite directions: it saw 50 failures, and
every event reached both subscribers. If it retries, each retry is a new POST with a new
event id, so `load-0` receives every event again with no way to recognise the repeat.

**Dead subscriber.** The producer got a fast 200 for every call. Half the deliveries failed.
The failure was reported, in the body of a 200, to the one party that cannot act on it: the
producer cannot resend to `load-1` alone, does not own `load-1`, and would have to parse the
body to notice. The service, which could try again, does nothing. 50 events were lost.

Both breaks are one flaw seen from two sides. The producer is made to wait on subscribers it
does not control, and is handed outcomes it cannot do anything about.

## Options

- **A. Keep inline delivery, shorten the per-subscriber timeout.** Smallest change. Caps the
  producer's wait, but the wait is still set by the slowest subscriber, a slow subscriber
  becomes a failed delivery, and there is still nobody to retry.
- **B. Reply immediately; deliver afterwards inside the API process.** Small change. Ingest
  latency stops depending on subscribers. Costs: deliveries live only in memory, so a restart
  loses whatever is unfinished, and nothing limits how many pile up.
- **C. Reply after saving to a database; a separate worker delivers.** The durable version of
  B, with a natural place for retries. Costs a database and a second program; it is stage 2.
- **D. Fail the request when any subscriber fails, so the producer retries.** Easy to explain.
  Pushes retries onto every producer and re-sends to the subscribers that already succeeded.

## Decision

**B now, then C at stage 2.** B fixes the contract with the smallest change; C is what makes
the contract true across a restart. The contract is decided here and does not change when
the implementation does:

- `POST /apps/{id}/events` returns **202 Accepted** with `{"id": ..., "status": "accepted"}`
  as soon as the event is recorded. It contacts no subscriber before replying.
- The reply promises *delivery*, not a result. The result does not exist yet when the reply
  is sent, so it is looked up afterwards: `GET /apps/{id}/events/{event_id}`.
- Trying a failed subscriber again is hookline's job, not the producer's. (Not built yet;
  stage 3.)

The deciding reason: the only thing a producer can act on is whether hookline took the event.
Anything slower or more detailed than that costs it latency and gives it nothing it can use.

## Consequences

- Ingest latency no longer depends on subscribers. The slow-subscriber run, before and after:

  | | inline | 202 + background |
  |---|---|---|
  | producer status | ReadTimeout ×50 | 202 ×50 |
  | ingest latency p50 | 5.012 s | 0.027 s |
  | ingest throughput | 2.0 events/s | 332 events/s |
  | delivered | 100/100 | 100/100 |
  | `load-0` latency p50 | 0.061 s | 0.120 s |
  | `load-1` latency p50 | 30.06 s | 30.12 s |

  The subscribers' experience is unchanged; only the producer's is. Delivery to the healthy
  endpoint now trails ingest slightly (0.12 s), because 50 events are accepted in 0.15 s and
  their deliveries then compete inside one process. The gap between "accepted" and
  "delivered" is the number every later stage watches.
- The producer no longer sees outcomes in the reply. It asks for them by event id.
- The 202 promises more than the code can keep. A restart forgets every app, endpoint and
  unfinished delivery, because all of it lives in the process's memory. That is on purpose,
  for now, and it is the first break of stage 2.
- Nothing limits how many deliveries run at once. Ingest can now outrun delivery with nothing
  pushing back.

## Promises, as of stage 1

- **To the producer:** a 202 means hookline will attempt to deliver this event to every
  endpoint the app had at that moment, and the producer does not need to send it again.
- **To the subscriber:** every event accepted for its app will be POSTed to it, with a
  `Webhook-Id` header carrying the event id.

Both are false across a restart. They are written down so stage 2 can make them true.
