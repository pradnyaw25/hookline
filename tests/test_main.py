"""Stage 1: ingest replies 202 at once; delivery happens in the background (ADR-0001)."""

import asyncio
import time

import httpx
import pytest

from hookline import main
from hookline.main import app
from tools.receiver import app as receiver


@pytest.fixture
async def client(monkeypatch):
    """Service client whose outbound HTTP goes straight to the in-process flaky receiver."""
    receiver.hooks.clear()
    main.apps.clear()
    main.endpoints.clear()
    main.events.clear()
    monkeypatch.setattr(
        main,
        "new_client",
        lambda: httpx.AsyncClient(transport=httpx.ASGITransport(app=receiver.app), timeout=5),
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://s") as c:
        yield c


async def settle() -> None:
    """Wait for every background delivery started so far."""
    if main.background:
        await asyncio.gather(*main.background)


async def make_app(client: httpx.AsyncClient, hooks: list[str]) -> str:
    app_id = (await client.post("/apps", json={"name": "shop"})).json()["id"]
    for h in hooks:
        r = await client.post(f"/apps/{app_id}/endpoints", json={"url": f"http://r/hook/{h}"})
        assert r.status_code == 200
    return app_id


async def test_health(client):
    assert (await client.get("/health")).json() == {"status": "ok"}


async def test_unknown_app_is_404(client):
    missing = "00000000-0000-0000-0000-000000000000"
    r = await client.post(f"/apps/{missing}/endpoints", json={"url": "http://r/hook/a"})
    assert r.status_code == 404
    r = await client.post(f"/apps/{missing}/events", json={"type": "t", "payload": {}})
    assert r.status_code == 404


async def test_malformed_event_is_422(client):
    app_id = await make_app(client, [])
    r = await client.post(f"/apps/{app_id}/events", json={"type": "t", "payload": "nope"})
    assert r.status_code == 422


async def test_event_is_accepted_then_delivered_to_every_endpoint(client):
    app_id = await make_app(client, ["a", "b"])
    payload = {"n": 1, "sent_at": time.time()}
    r = await client.post(
        f"/apps/{app_id}/events", json={"type": "order.created", "payload": payload}
    )
    assert r.status_code == 202 and r.json()["status"] == "accepted"
    event_id = r.json()["id"]
    await settle()
    record = (await client.get(f"/apps/{app_id}/events/{event_id}")).json()
    assert record["status"] == "done"
    assert [d["status"] for d in record["deliveries"]] == ["success", "success"]
    for h in ("a", "b"):
        hook = receiver.hooks[h]
        assert event_id in hook.succeeded_events, "Webhook-Id carries the event id"
        assert hook.recent[-1].body == payload, "the subscriber gets the bare payload"


async def test_ingest_does_not_wait_for_a_slow_endpoint(client):
    receiver.hooks["slow"].behavior = receiver.Behavior(delay_ms=500)
    app_id = await make_app(client, ["slow"])
    t0 = time.perf_counter()
    r = await client.post(f"/apps/{app_id}/events", json={"type": "t", "payload": {}})
    assert r.status_code == 202 and time.perf_counter() - t0 < 0.25
    record = (await client.get(f"/apps/{app_id}/events/{r.json()['id']}")).json()
    assert record["status"] in ("accepted", "delivering"), "the reply came before the result"
    await settle()
    assert receiver.hooks["slow"].succeeded == 1


async def test_failed_endpoint_is_recorded_and_does_not_fail_the_others(client):
    receiver.hooks["bad"].behavior = receiver.Behavior(down=True)
    app_id = await make_app(client, ["ok", "bad"])
    r = await client.post(f"/apps/{app_id}/events", json={"type": "t", "payload": {}})
    assert r.status_code == 202
    await settle()
    record = (await client.get(f"/apps/{app_id}/events/{r.json()['id']}")).json()
    ok, bad = record["deliveries"]
    assert (ok["status"], ok["status_code"]) == ("success", 200)
    assert (bad["status"], bad["status_code"], bad["error"]) == ("failed", 503, None)
    assert receiver.hooks["bad"].total_requests == 1, "stage 1 does not retry"


async def test_event_with_no_endpoints_is_accepted(client):
    app_id = await make_app(client, [])
    r = await client.post(f"/apps/{app_id}/events", json={"type": "t", "payload": {}})
    assert r.status_code == 202
    await settle()
    record = (await client.get(f"/apps/{app_id}/events/{r.json()['id']}")).json()
    assert record["status"] == "done" and record["deliveries"] == []


async def test_unknown_event_is_404(client):
    app_id = await make_app(client, [])
    assert (await client.get(f"/apps/{app_id}/events/nope")).status_code == 404
