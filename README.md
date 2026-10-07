# hookline

An outbound webhook delivery service, built one stage at a time to learn system design.

Producers POST an event. hookline fans it out to every subscribed endpoint, signs each request,
retries on a backoff schedule, dead-letters what never succeeds, and keeps a log of every attempt.

The product is secondary. Each stage builds the obvious version of something, breaks it under a
reproducible load, and only then introduces the pattern that fixes it. The record of those
decisions lives in `docs/adr/`. The full course is in [docs/ROADMAP.md](docs/ROADMAP.md).

## Layout

```
src/hookline/      the service — written stage by stage, starting from a /health stub
tools/receiver/    flaky receiver: a webhook target you can make slow, broken or dead on command
tools/load/        load script: pushes events through the service and reports what arrived
docs/ROADMAP.md    every stage: what to build, how to break it, what it teaches
docs/stages/       the detailed spec for each stage as it is reached
docs/adr/          one architecture decision record per decision, written from evidence
tests/             tests for the tooling; add your own for the service
```

## Setup

Requires Python 3.12+. Postgres (stage 2 onward) via Docker Desktop / OrbStack with the included
`docker-compose.yml`, or a local `brew install postgresql@17`.

```bash
make venv
make test
```

## Running a stage

Three terminals:

```bash
make receiver     # the target, on :9000
make api          # your service, on :8000
make load ARGS="--events 200 --concurrency 20"
```

Shape the receiver's behavior with its control API, for example a 30-second stall:

```bash
curl -X PUT localhost:9000/control/load-0 -H 'content-type: application/json' -d '{"delay_ms": 30000}'
```

`PUT /control/<name>` takes any of: `status_code`, `fail_attempts`, `fail_rate`, `delay_ms`,
`jitter_ms`, `hang`, `down`, `max_concurrent`, `max_body_bytes`, `secret`.
`GET /control/<name>` shows behavior and counters. `DELETE` resets both; the load script
zeroes the counters itself before each run and keeps the behavior.

## Writing an ADR

Copy `docs/adr/0000-template.md` to the next number. Write it *after* reproducing the failure
and *before* writing the fix. Numbers in the ADR should come from the load script's output.
