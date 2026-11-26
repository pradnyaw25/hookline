"""hookline — the service.

Stage 1: ``POST /apps/{id}/events`` records the event, replies 202 at once, and delivers to
every endpoint of the app in a background task inside this process. No retries, no database;
a restart forgets everything (ADR-0001; stage 2 fixes that).

Spec: docs/stages/01-synchronous-delivery.md
Why 202: docs/adr/0001-decouple-ingest-from-delivery.md
"""

import asyncio
from typing import Any
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class AppIn(BaseModel):
    name: str


class EndpointIn(BaseModel):
    url: str


class EventIn(BaseModel):
    type: str
    payload: dict[str, Any]


app = FastAPI(title="hookline")

apps: dict[str, dict[str, str]] = {}
endpoints: dict[str, list[dict[str, str]]] = {}
events: dict[str, dict[str, Any]] = {}  # event_id -> record, filled in by the background task
background: set[asyncio.Task[None]] = set()

# How long we wait for one subscriber to answer.
DELIVERY_TIMEOUT_S = 60.0


def new_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=DELIVERY_TIMEOUT_S)


async def deliver(
    client: httpx.AsyncClient, endpoint: dict[str, str], event_id: str, payload: dict[str, Any]
) -> dict[str, Any]:
    res: dict[str, Any] = {
        "endpoint_id": endpoint["id"],
        "event_id": event_id,
        "url": endpoint["url"],
        "status": "failed",
        "status_code": None,
        "error": None,
    }
    try:
        r = await client.post(endpoint["url"], json=payload, headers={"Webhook-Id": event_id})
    except httpx.HTTPError as e:
        res["error"] = f"{type(e).__name__}: {e}"
        return res
    res["status"] = "success" if r.is_success else "failed"
    res["status_code"] = r.status_code
    return res


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/apps")
async def create_app(body: AppIn) -> dict[str, str]:
    app_id = str(uuid4())
    apps[app_id] = {"id": app_id, "name": body.name}
    endpoints[app_id] = []
    return apps[app_id]


@app.post("/apps/{app_id}/endpoints")
async def create_endpoint(app_id: str, body: EndpointIn) -> dict[str, str]:
    if app_id not in apps:
        raise HTTPException(status_code=404, detail="app not found")
    endpoint = {"id": str(uuid4()), "url": body.url}
    endpoints[app_id].append(endpoint)
    return endpoint


async def deliver_all(
    event_id: str, targets: list[dict[str, str]], payload: dict[str, Any]
) -> None:
    """Runs after the producer already has its 202. Records the outcome on the event."""
    record = events[event_id]
    record["status"] = "delivering"
    async with new_client() as client:
        # Start one delivery per endpoint, then wait until every one of them has answered.
        results = await asyncio.gather(
            *(deliver(client, endpoint, event_id, payload) for endpoint in targets)
        )
    record["deliveries"] = results
    record["status"] = "done"


@app.post("/apps/{app_id}/events", status_code=202)
async def create_event(app_id: str, body: EventIn) -> dict[str, str]:
    """Take responsibility for the event and reply. Delivery happens after the reply."""
    if app_id not in apps:
        raise HTTPException(status_code=404, detail="app not found")
    event_id = str(uuid4())
    events[event_id] = {
        "id": event_id,
        "app_id": app_id,
        "type": body.type,
        "status": "accepted",
        "deliveries": [],
    }
    # The endpoints the app has right now are the ones this event is owed to.
    task = asyncio.create_task(deliver_all(event_id, list(endpoints[app_id]), body.payload))
    background.add(task)  # keep a reference, or Python may discard the task mid-flight
    task.add_done_callback(background.discard)
    return {"id": event_id, "status": "accepted"}


@app.get("/apps/{app_id}/events/{event_id}")
async def get_event(app_id: str, event_id: str) -> dict[str, Any]:
    """What happened to an event: accepted, delivering, or done with per-endpoint results."""
    record = events.get(event_id)
    if record is None or record["app_id"] != app_id:
        raise HTTPException(status_code=404, detail="event not found")
    return record
