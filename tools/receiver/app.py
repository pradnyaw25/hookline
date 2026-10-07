"""Flaky receiver: a webhook target you can make slow, broken, or dead on command.

Run:   make receiver        (port 9000)
Point an endpoint at:  http://localhost:9000/hook/<name>
Configure it:          PUT /control/<name>   with a Behavior JSON body
Inspect it:            GET /control/<name>   (behavior + stats)
Reset it:              DELETE /control/<name>            (behavior and counters)
                       POST /control/<name>/reset-stats (counters only)

Each ``<name>`` is an independent hook with its own behavior and counters, so one load run
can have a healthy endpoint and a broken one side by side.

Everything lives in memory. Restarting the receiver clears it.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, Field

from .signing import verify

GLOBAL_KEY = "__no_webhook_id__"
INSPECT_RING = 1000  # how many recent requests to keep for GET /received


class Behavior(BaseModel):
    """How a hook responds. All fields optional; the default hook is a fast, healthy 200."""

    status_code: int = Field(500, description="Status returned when a request is failed")
    fail_attempts: int = Field(
        0,
        description="Fail the first N attempts of each event (keyed by Webhook-Id header). "
        "Without that header it is the first N requests overall.",
    )
    fail_rate: float = Field(0.0, ge=0, le=1, description="Probability any request fails")
    delay_ms: int = Field(0, ge=0, description="Sleep before responding")
    jitter_ms: int = Field(0, ge=0, description="Extra random 0..N ms on top of delay_ms")
    hang: bool = Field(
        False, description="Never respond; hold the connection until the client gives up"
    )
    down: bool = Field(False, description="Respond 503 immediately to everything")
    max_concurrent: int = Field(
        0, ge=0, description="Above this many in-flight requests respond 429. 0 = unlimited"
    )
    max_body_bytes: int = Field(
        0, ge=0, description="Bodies larger than this get 413. 0 = unlimited"
    )
    secret: str | None = Field(
        None, description="When set, verify the Standard Webhooks signature; bad → 401"
    )


@dataclass
class Received:
    at: float
    event_id: str | None
    attempt_no: int
    status: int
    latency_s: float | None
    headers: dict[str, str]
    body: Any


@dataclass
class Hook:
    behavior: Behavior = field(default_factory=Behavior)
    recent: deque[Received] = field(default_factory=lambda: deque(maxlen=INSPECT_RING))
    attempts_by_event: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    succeeded_events: set[str] = field(default_factory=set)
    latencies: list[float] = field(default_factory=list)  # first success per event only
    in_flight: int = 0
    max_in_flight: int = 0
    total_requests: int = 0
    succeeded: int = 0
    failed: int = 0
    duplicates: int = 0  # a 2xx for an event id that already got a 2xx
    rejected_down: int = 0
    rejected_concurrency: int = 0
    rejected_size: int = 0
    bad_signature: int = 0
    hung: int = 0
    first_request_at: float | None = None
    last_request_at: float | None = None

    def stats(self) -> dict[str, Any]:
        lat = sorted(self.latencies)

        def pct(p: float) -> float | None:
            if not lat:
                return None
            return round(lat[min(len(lat) - 1, int(p * len(lat)))], 4)

        return {
            "total_requests": self.total_requests,
            "unique_events_succeeded": len(self.succeeded_events),
            "succeeded": self.succeeded,
            "failed": self.failed,
            "duplicates": self.duplicates,
            "rejected_down": self.rejected_down,
            "rejected_concurrency": self.rejected_concurrency,
            "rejected_size": self.rejected_size,
            "bad_signature": self.bad_signature,
            "hung": self.hung,
            "in_flight": self.in_flight,
            "max_in_flight": self.max_in_flight,
            "attempts_per_event_max": max(self.attempts_by_event.values(), default=0),
            "latency_s": {
                "n": len(lat),
                "p50": pct(0.5),
                "p95": pct(0.95),
                "p99": pct(0.99),
                "max": lat[-1] if lat else None,
            },
            "first_request_at": self.first_request_at,
            "last_request_at": self.last_request_at,
        }


hooks: dict[str, Hook] = defaultdict(Hook)
app = FastAPI(title="hookline flaky receiver")


def _record(
    hook: Hook,
    status: int,
    event_id: str | None,
    attempt_no: int,
    headers: dict[str, str],
    raw: bytes,
) -> None:
    body: Any
    latency = None
    try:
        body = json.loads(raw) if raw else None
        sent_at = body.get("sent_at") if isinstance(body, dict) else None
        if isinstance(sent_at, int | float) and 200 <= status < 300:
            latency = time.time() - sent_at
    except (ValueError, AttributeError):
        body = raw.decode(errors="replace")
    hook.recent.append(Received(time.time(), event_id, attempt_no, status, latency, headers, body))
    if latency is not None and (event_id is None or event_id not in hook.succeeded_events):
        hook.latencies.append(latency)


@app.post("/hook/{name}")
async def hook_endpoint(name: str, request: Request) -> Response:
    hook = hooks[name]
    b = hook.behavior
    now = time.time()
    hook.total_requests += 1
    hook.first_request_at = hook.first_request_at or now
    hook.last_request_at = now

    if b.down:
        hook.rejected_down += 1
        return Response(status_code=503)
    if b.max_concurrent and hook.in_flight >= b.max_concurrent:
        hook.rejected_concurrency += 1
        return Response(status_code=429, headers={"Retry-After": "1"})

    hook.in_flight += 1
    hook.max_in_flight = max(hook.max_in_flight, hook.in_flight)
    try:
        raw = await request.body()
        headers = {
            k.lower(): v
            for k, v in request.headers.items()
            if k.lower().startswith("webhook-") or k.lower() in ("content-type", "user-agent")
        }
        event_id = headers.get("webhook-id")
        key = event_id or GLOBAL_KEY
        hook.attempts_by_event[key] += 1
        attempt_no = hook.attempts_by_event[key]

        if b.max_body_bytes and len(raw) > b.max_body_bytes:
            hook.rejected_size += 1
            _record(hook, 413, event_id, attempt_no, headers, raw)
            return Response(status_code=413)

        if b.hang:
            hook.hung += 1
            while not await request.is_disconnected():  # noqa: ASYNC110 — no event to await
                await asyncio.sleep(0.25)
            return Response(status_code=499)

        delay = b.delay_ms + (random.uniform(0, b.jitter_ms) if b.jitter_ms else 0)
        if delay:
            await asyncio.sleep(delay / 1000)

        if b.secret is not None:
            reason = verify(b.secret, headers, raw)
            if reason:
                hook.bad_signature += 1
                _record(hook, 401, event_id, attempt_no, headers, raw)
                return Response(status_code=401, content=reason)

        if attempt_no <= b.fail_attempts or (b.fail_rate and random.random() < b.fail_rate):
            hook.failed += 1
            _record(hook, b.status_code, event_id, attempt_no, headers, raw)
            return Response(status_code=b.status_code)

        _record(hook, 200, event_id, attempt_no, headers, raw)
        hook.succeeded += 1
        if event_id is not None:
            if event_id in hook.succeeded_events:
                hook.duplicates += 1
            hook.succeeded_events.add(event_id)
        return Response(status_code=200)
    finally:
        hook.in_flight -= 1


# ---- control plane -------------------------------------------------------------------------


@app.get("/control")
async def list_hooks() -> dict[str, Any]:
    return {
        name: {"behavior": h.behavior.model_dump(), "stats": h.stats()} for name, h in hooks.items()
    }


@app.get("/control/{name}")
async def get_hook(name: str) -> dict[str, Any]:
    h = hooks[name]
    return {"behavior": h.behavior.model_dump(), "stats": h.stats()}


@app.put("/control/{name}")
async def set_behavior(name: str, behavior: Behavior) -> dict[str, Any]:
    """Replace the behavior. Stats are kept; use DELETE to reset them."""
    hooks[name].behavior = behavior
    return {"behavior": behavior.model_dump()}


@app.patch("/control/{name}")
async def patch_behavior(name: str, patch: dict[str, Any]) -> dict[str, Any]:
    h = hooks[name]
    h.behavior = h.behavior.model_copy(
        update=Behavior.model_validate({**h.behavior.model_dump(), **patch}).model_dump()
    )
    return {"behavior": h.behavior.model_dump()}


@app.delete("/control/{name}")
async def reset_hook(name: str) -> dict[str, str]:
    """Full reset: behavior back to default and counters to zero."""
    hooks[name] = Hook()
    return {"reset": name}


@app.post("/control/{name}/reset-stats")
async def reset_stats(name: str) -> dict[str, str]:
    """Zero the counters but keep the behavior. The load script calls this before each run."""
    hooks[name] = Hook(behavior=hooks[name].behavior)
    return {"reset_stats": name}


@app.get("/control/{name}/stats")
async def get_stats(name: str) -> dict[str, Any]:
    return hooks[name].stats()


@app.get("/control/{name}/received")
async def get_received(name: str, limit: int = 100) -> list[dict[str, Any]]:
    recent = list(hooks[name].recent)[-limit:]
    return [r.__dict__ for r in recent]
