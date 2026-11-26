"""hookline — the service. This file is yours.

Stage 1 spec: docs/stages/01-synchronous-delivery.md
The load script and flaky receiver expect the API contract described there.
"""

from typing import Any
from uuid import uuid4

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


@app.post("/apps/{app_id}/events")
async def create_event(app_id: str, body: EventIn) -> dict[str, Any]:
    if app_id not in apps:
        raise HTTPException(status_code=404, detail="app not found")
    event = {"id": str(uuid4()), "type": body.type, "payload": body.payload}
    return event
