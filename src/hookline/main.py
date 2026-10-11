"""hookline — the service. This file is yours.

Stage 2: ``POST /apps/{id}/events`` writes the event and one delivery row per endpoint in a
single transaction, then returns 202. A separate worker process (``hookline.worker``) polls
the ``deliveries`` table and sends. The API never talks to a subscriber.

Spec: docs/stages/02-postgres-queue.md
Why 202: docs/adr/0001-decouple-ingest-from-delivery.md
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

from hookline import db

# ---- models ------------------------------------------------------------------------------


class AppIn(BaseModel):
    name: str


class AppOut(BaseModel):
    id: UUID
    name: str


class EndpointIn(BaseModel):
    url: str


class EndpointOut(BaseModel):
    id: UUID
    url: str


class EventIn(BaseModel):
    type: str
    payload: dict[str, Any]


class EventAccepted(BaseModel):
    id: UUID
    status: str = "accepted"


class DeliveryOut(BaseModel):
    id: int
    endpoint_id: UUID
    url: str
    status: str
    attempts: int
    last_status: int | None
    last_error: str | None


class EventOut(BaseModel):
    id: UUID
    app_id: UUID
    type: str
    deliveries: list[DeliveryOut]


# ---- app ---------------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = db.make_pool()
    await app.state.pool.open()
    try:
        yield
    finally:
        await app.state.pool.close()


app = FastAPI(title="hookline", lifespan=lifespan)


def pool() -> AsyncConnectionPool:
    return app.state.pool


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/apps", response_model=AppOut)
async def create_app(body: AppIn) -> AppOut:
    app_id = uuid4()
    async with pool().connection() as conn:
        await conn.execute("INSERT INTO apps (id, name) VALUES (%s, %s)", (app_id, body.name))
    return AppOut(id=app_id, name=body.name)


@app.post("/apps/{app_id}/endpoints", response_model=EndpointOut)
async def create_endpoint(app_id: UUID, body: EndpointIn) -> EndpointOut:
    ep_id = uuid4()
    async with pool().connection() as conn:
        if not await (await conn.execute("SELECT 1 FROM apps WHERE id = %s", (app_id,))).fetchone():
            raise HTTPException(404, "app not found")
        await conn.execute(
            "INSERT INTO endpoints (id, app_id, url) VALUES (%s, %s, %s)", (ep_id, app_id, body.url)
        )
    return EndpointOut(id=ep_id, url=body.url)


@app.post("/apps/{app_id}/events", response_model=EventAccepted, status_code=202)
async def create_event(app_id: UUID, body: EventIn) -> EventAccepted:
    """Accept responsibility: the event and its delivery rows commit together, then 202."""
    event_id = uuid4()
    async with pool().connection() as conn:  # one transaction; commits on exit
        if not await (await conn.execute("SELECT 1 FROM apps WHERE id = %s", (app_id,))).fetchone():
            raise HTTPException(404, "app not found")
        await conn.execute(
            "INSERT INTO events (id, app_id, type, payload) VALUES (%s, %s, %s, %s)",
            (event_id, app_id, body.type, Jsonb(body.payload)),
        )
        await conn.execute(
            "INSERT INTO deliveries (event_id, endpoint_id)"
            " SELECT %s, id FROM endpoints WHERE app_id = %s ORDER BY created_at",
            (event_id, app_id),
        )
    return EventAccepted(id=event_id)


@app.get("/apps/{app_id}/events/{event_id}", response_model=EventOut)
async def get_event(app_id: UUID, event_id: UUID) -> EventOut:
    async with pool().connection() as conn:
        ev = await (
            await conn.execute(
                "SELECT id, app_id, type FROM events WHERE id = %s AND app_id = %s",
                (event_id, app_id),
            )
        ).fetchone()
        if ev is None:
            raise HTTPException(404, "event not found")
        rows = await (
            await conn.execute(
                "SELECT d.id, d.endpoint_id, e.url, d.status, d.attempts, d.last_status,"
                " d.last_error FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id"
                " WHERE d.event_id = %s ORDER BY d.id",
                (event_id,),
            )
        ).fetchall()
    return EventOut(**ev, deliveries=[DeliveryOut(**r) for r in rows])
