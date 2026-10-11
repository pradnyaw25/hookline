"""hookline — the service. This file is yours.

Stage 1: ``POST /apps/{id}/events`` records the event, returns 202, and delivers to every
endpoint of the app in a background task inside this process. No retries, no database;
a restart loses whatever has not been delivered yet (ADR-0001, fixed at stage 2).

Spec: docs/stages/01-synchronous-delivery.md
Why 202: docs/adr/0001-decouple-ingest-from-delivery.md
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # How long we wait for a subscriber to answer. Stage 1 waits as long as it takes.
    delivery_timeout_s: float = 60.0


settings = Settings()

# ---- models ------------------------------------------------------------------------------


class AppIn(BaseModel):
    name: str


class AppOut(BaseModel):
    id: str
    name: str


class EndpointIn(BaseModel):
    url: str


class EndpointOut(BaseModel):
    id: str
    url: str


class EventIn(BaseModel):
    type: str
    payload: dict[str, Any]


class DeliveryResult(BaseModel):
    endpoint_id: str
    url: str
    ok: bool
    status: int | None = None
    error: str | None = None
    elapsed_ms: int


class EventAccepted(BaseModel):
    id: str
    status: str = "accepted"


class EventRecord(BaseModel):
    id: str
    app_id: str
    type: str
    status: str  # accepted | delivering | done
    deliveries: list[DeliveryResult] = []


# ---- in-memory storage (goes away at stage 2) -------------------------------------------

apps: dict[str, dict[str, Any]] = {}
endpoints: dict[str, list[dict[str, Any]]] = {}  # app_id -> [{"id", "url"}]
events: dict[str, EventRecord] = {}  # event_id -> record, filled in by the background task
background: set[asyncio.Task[None]] = set()  # keep references so tasks are not collected


# ---- app ---------------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(timeout=settings.delivery_timeout_s)
    try:
        yield
    finally:
        await app.state.http.aclose()


app = FastAPI(title="hookline", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/apps", response_model=AppOut)
async def create_app(body: AppIn) -> AppOut:
    app_id = str(uuid4())
    apps[app_id] = {"id": app_id, "name": body.name}
    endpoints[app_id] = []
    return AppOut(id=app_id, name=body.name)


@app.post("/apps/{app_id}/endpoints", response_model=EndpointOut)
async def create_endpoint(app_id: str, body: EndpointIn) -> EndpointOut:
    if app_id not in apps:
        raise HTTPException(404, "app not found")
    ep = {"id": str(uuid4()), "url": body.url}
    endpoints[app_id].append(ep)
    return EndpointOut(**ep)


async def deliver(
    http: httpx.AsyncClient, event_id: str, event_type: str, ep: dict[str, Any], body: dict
) -> DeliveryResult:
    headers = {
        "Webhook-Id": event_id,
        "Webhook-Timestamp": str(int(time.time())),
        "X-Hookline-Event-Type": event_type,
    }
    t0 = time.perf_counter()
    try:
        r = await http.post(ep["url"], json=body, headers=headers)
        return DeliveryResult(
            endpoint_id=ep["id"],
            url=ep["url"],
            ok=r.is_success,
            status=r.status_code,
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
        )
    except httpx.HTTPError as e:
        return DeliveryResult(
            endpoint_id=ep["id"],
            url=ep["url"],
            ok=False,
            error=f"{type(e).__name__}: {e}",
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
        )


async def deliver_all(event_id: str, payload: dict[str, Any]) -> None:
    rec = events[event_id]
    rec.status = "delivering"
    http: httpx.AsyncClient = app.state.http
    results = await asyncio.gather(
        *(deliver(http, event_id, rec.type, ep, payload) for ep in endpoints[rec.app_id])
    )
    rec.deliveries = list(results)
    rec.status = "done"


@app.post("/apps/{app_id}/events", response_model=EventAccepted, status_code=202)
async def create_event(app_id: str, body: EventIn) -> EventAccepted:
    """Accept responsibility for the event and return. Delivery happens after the response."""
    if app_id not in apps:
        raise HTTPException(404, "app not found")
    event_id = str(uuid4())
    events[event_id] = EventRecord(id=event_id, app_id=app_id, type=body.type, status="accepted")
    task = asyncio.create_task(deliver_all(event_id, body.payload))
    background.add(task)
    task.add_done_callback(background.discard)
    return EventAccepted(id=event_id)


@app.get("/apps/{app_id}/events/{event_id}", response_model=EventRecord)
async def get_event(app_id: str, event_id: str) -> EventRecord:
    rec = events.get(event_id)
    if rec is None or rec.app_id != app_id:
        raise HTTPException(404, "event not found")
    return rec
