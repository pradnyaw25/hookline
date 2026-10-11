# ADR-0001: Ingest and delivery are decoupled; `POST /events` returns 202 and promises acceptance, not delivery

**Stage:** 1  **Date:** 2026-10-10  **Status:** accepted

## What broke

Stage 1 delivers inline: `POST /apps/{id}/events` POSTs to every endpoint, waits for all of
them, and returns 200 with per-endpoint results. Two endpoints, one healthy (`load-0`), one
made to stall for 30 seconds (`load-1`).

```bash
make load ARGS="--events 50 --concurrency 10 --endpoints 2"                           # baseline
curl -X PUT localhost:9000/control/load-1 -H 'content-type: application/json' -d '{"delay_ms": 30000}'
make load ARGS="--events 50 --concurrency 10 --endpoints 2 --timeout 5 --wait 90"     # break
```

| | baseline | `load-1` stalls 30 s | `load-1` down (503) |
|---|---|---|---|
| ingest wall | 0.15 s | 25.07 s | 0.14 s |
| ingest throughput | 331 events/s | 2.0 events/s | 354 events/s |
| ingest latency p50 / p99 | 0.025 s / 0.073 s | 5.013 s / 5.016 s | 0.024 s / 0.068 s |
| producer status | 200 ×50 | **ReadTimeout ×50** | 200 ×50 |
| delivered (receiver) | 100/100 | **100/100** | 50/100, 50 rejected 503 |
| `load-0` latency p50 | 0.018 s | 0.025 s | 0.014 s |
| `load-1` latency p50 | 0.018 s | 30.026 s | never |
| max in-flight at `load-1` | 2 | 50 | — |

What the numbers say, in the order the spec asks:

- **The healthy endpoint was fine; the producer was not.** `load-0` received every event in
  about 25 ms, same as baseline. But ingest latency for the producer went from 25 ms to the
  producer's own 5 s timeout on every single call, because the response waits for the slowest
  subscriber. One slow subscriber set the latency for the whole app.
- **The producer's view was wrong in both directions.** It saw 50 timeouts and would reasonably
  conclude nothing was delivered. In fact everything was delivered: `load-0` immediately,
  `load-1` thirty seconds later. The service kept working on requests whose client had already
  hung up, and the per-endpoint results it computed went to nobody. A producer that retries on
  timeout would have caused 50 duplicate deliveries to each endpoint.
- **With `load-1` down**, the producer got a fast 200 whose body said one endpoint got a 503.
  The service's answer is honest but useless: the producer has no way to act on it (it cannot
  retry to just that endpoint), and the service does nothing further. 50 events are lost unless
  the producer resends them to everyone. A 200 that means "half of this failed and I have
  given up" is not a success.
- **What the producer is actually asking** is "accept responsibility for this event", not
  "deliver this now and tell me how it went". It cannot act on per-endpoint outcomes, it does
  not want to wait on subscribers it does not control, and the subscribers' availability is
  not its problem. The synchronous design makes the producer pay for every subscriber's
  latency and failures while giving it nothing it can use.

## Options

1. **Keep synchronous delivery, add a per-subscriber timeout.** Cheap. Caps the producer's
   wait at the timeout. Still couples ingest latency to subscriber latency (just a smaller
   number), still cannot retry without the producer resending, still turns a slow subscriber
   into a failed delivery. Would need to be undone by stage 4 (retries) anyway.
2. **Accept and return 202, deliver in the background inside the API process.** Ingest
   latency becomes independent of subscribers. Costs: delivery state lives in process memory,
   so a restart or crash loses every in-flight and queued delivery, and there is no back-pressure
   (every accepted event becomes a task). Buys the right contract now, at the smallest change.
3. **Accept, persist the delivery as a row, and have a separate worker deliver it.** The
   durable version of option 2. Costs a database and a worker process, which is stage 2's
   entire subject. Buys survival across restarts and a place to hang retries and leases.
4. **Return 200 only after all subscribers succeed, 5xx otherwise, and let the producer retry.**
   Pushes retry and idempotency onto every producer and, as the stall run shows, produces
   duplicates to the endpoints that did succeed. Rejected.

## Decision

Option 2 now, option 3 at stage 2. The contract is decided here and does not change when the
implementation does:

- `POST /apps/{id}/events` returns **202 Accepted** with `{"id": ..., "status": "accepted"}`
  once the event has been recorded for delivery. It does not contact any subscriber before
  responding and does not report per-endpoint outcomes.
- 202 means: hookline has taken responsibility for delivering this event to every endpoint the
  app had at ingest time. Delivery outcomes are the service's to track and expose separately
  (stage 8), not the producer's to interpret from the ingest response.

The one reason: the producer cannot act on anything but "accepted" or "not accepted", so
anything slower or more detailed than that costs it latency and gives it nothing.

## Consequences

- Ingest latency no longer depends on subscribers. The stall scenario re-run after the change:

  | | sync, `load-1` stalls 30 s | 202 + background, `load-1` stalls 30 s |
  |---|---|---|
  | ingest wall | 25.07 s | 0.04 s |
  | ingest latency p50 / p99 | 5.013 s / 5.016 s | 0.006 s / 0.015 s |
  | producer status | ReadTimeout ×50 | 202 ×50 |
  | delivered | 100/100 | 100/100 |
  | `load-0` latency p50 | 0.025 s | 0.032 s |

  Ingest is now faster than the healthy baseline too (p50 6 ms vs 25 ms), because the
  response no longer waits for even a fast subscriber.
- Delivery now lags ingest, visibly. In the healthy run after the change, 50 events were
  accepted in 50 ms and the receiver-side latency p50 rose from 18 ms to 101 ms: deliveries
  queue up behind each other inside the process. That gap between "accepted" and "delivered"
  is the quantity every later stage measures.
- The producer loses the ability to see delivery results inline. Until stage 8, the only
  window is `GET /apps/{id}/events/{event_id}`, an in-memory record of attempts.
- **Durability is now a lie we are telling on purpose.** 202 promises delivery; the in-process
  task queue cannot keep that promise across a restart. The signal that it is time: kill the
  API with events accepted but not yet delivered, and count what arrives. That is the opening
  move of stage 2.
- There is no limit on background tasks, so ingest can now outrun delivery without anything
  pushing back. That is stage 6.
- Nothing drains in-flight deliveries on shutdown. That is stage 18.

## Promises, as of stage 1

- **To the producer:** a 202 means hookline will attempt to deliver this event to every
  endpoint subscribed at the time of the POST, and the producer does not need to resend it.
- **To the subscriber:** every event accepted for its app will be POSTed to it at least once,
  with a `Webhook-Id` header it can use to recognise repeats.

Both sentences are currently false under a process restart. They are written down so that
stage 2 can make them true.
