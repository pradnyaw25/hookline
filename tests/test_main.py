"""Stage 1: ingest returns 202 at once; delivery happens in the background (ADR-0001)."""

import asyncio
import time

import httpx
import pytest

from hookline import main
from hookline.main import app
from tools.receiver import app as receiver


@pytest.fixture
async def client():
    """Service client whose outbound HTTP goes straight to the in-process flaky receiver."""
    receiver.hooks.clear()
    app.state.http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=receiver.app), base_url="http://r", timeout=5
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://s") as c:
        yield c
    await app.state.http.aclose()


async def settle() -> None:
    """Wait for every background delivery started so far."""
    if main.background:
        await asyncio.gather(*main.background)


async def test_health(client):
    assert (await client.get("/health")).json() == {"status": "ok"}


async def test_create_app_and_endpoint(client):
    r = await client.post("/apps", json={"name": "shop"})
    assert r.status_code == 200
    app_id = r.json()["id"]
    r = await client.post(f"/apps/{app_id}/endpoints", json={"url": "http://r/hook/a"})
    assert r.status_code == 200 and r.json()["url"] == "http://r/hook/a"
    r = await client.post("/apps/nope/endpoints", json={"url": "http://r/hook/a"})
    assert r.status_code == 404


async def test_event_is_accepted_then_delivered_to_every_endpoint(client):
    app_id = (await client.post("/apps", json={"name": "shop"})).json()["id"]
    for name in ("a", "b"):
        await client.post(f"/apps/{app_id}/endpoints", json={"url": f"http://r/hook/{name}"})
    r = await client.post(
        f"/apps/{app_id}/events",
        json={"type": "order.created", "payload": {"n": 1, "sent_at": time.time()}},
    )
    assert r.status_code == 202
    assert r.json()["status"] == "accepted"
    event_id = r.json()["id"]
    await settle()
    rec = (await client.get(f"/apps/{app_id}/events/{event_id}")).json()
    assert rec["status"] == "done"
    assert len(rec["deliveries"]) == 2 and all(d["ok"] for d in rec["deliveries"])
    for name in ("a", "b"):
        s = receiver.hooks[name]
        assert s.succeeded == 1
        # the receiver keys its counters on Webhook-Id, which must be the event id
        assert event_id in s.succeeded_events


async def test_ingest_does_not_wait_for_a_slow_endpoint(client):
    receiver.hooks["slow"].behavior = receiver.Behavior(delay_ms=500)
    app_id = (await client.post("/apps", json={"name": "shop"})).json()["id"]
    await client.post(f"/apps/{app_id}/endpoints", json={"url": "http://r/hook/slow"})
    t0 = time.perf_counter()
    r = await client.post(f"/apps/{app_id}/events", json={"type": "t", "payload": {}})
    assert r.status_code == 202 and time.perf_counter() - t0 < 0.25
    await settle()
    assert receiver.hooks["slow"].succeeded == 1


async def test_failed_endpoint_is_reported_not_retried(client):
    receiver.hooks["bad"].behavior = receiver.Behavior(down=True)
    app_id = (await client.post("/apps", json={"name": "shop"})).json()["id"]
    await client.post(f"/apps/{app_id}/endpoints", json={"url": "http://r/hook/ok"})
    await client.post(f"/apps/{app_id}/endpoints", json={"url": "http://r/hook/bad"})
    r = await client.post(f"/apps/{app_id}/events", json={"type": "t", "payload": {}})
    assert r.status_code == 202
    await settle()
    rec = (await client.get(f"/apps/{app_id}/events/{r.json()['id']}")).json()
    by_url = {d["url"]: d for d in rec["deliveries"]}
    assert by_url["http://r/hook/ok"]["ok"] is True
    assert by_url["http://r/hook/bad"]["ok"] is False
    assert by_url["http://r/hook/bad"]["status"] == 503
    assert receiver.hooks["bad"].total_requests == 1, "stage 1 does not retry"


async def test_unknown_event_is_404(client):
    app_id = (await client.post("/apps", json={"name": "shop"})).json()["id"]
    assert (await client.get(f"/apps/{app_id}/events/nope")).status_code == 404
