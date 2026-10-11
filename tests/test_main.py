"""Stage 2: ingest commits the event and its delivery rows, then returns 202."""

import httpx
import pytest

from hookline.main import app


@pytest.fixture
async def client(pool):
    app.state.pool = pool
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://s") as c:
        yield c


async def test_health(client):
    assert (await client.get("/health")).json() == {"status": "ok"}


async def test_create_app_and_endpoint(client):
    r = await client.post("/apps", json={"name": "shop"})
    assert r.status_code == 200
    app_id = r.json()["id"]
    r = await client.post(f"/apps/{app_id}/endpoints", json={"url": "http://r/hook/a"})
    assert r.status_code == 200 and r.json()["url"] == "http://r/hook/a"
    r = await client.post(
        "/apps/00000000-0000-0000-0000-000000000000/endpoints", json={"url": "http://r/hook/a"}
    )
    assert r.status_code == 404


async def test_event_creates_one_pending_delivery_per_endpoint(client, pool):
    app_id = (await client.post("/apps", json={"name": "shop"})).json()["id"]
    for name in ("a", "b"):
        await client.post(f"/apps/{app_id}/endpoints", json={"url": f"http://r/hook/{name}"})
    r = await client.post(f"/apps/{app_id}/events", json={"type": "t", "payload": {"n": 1}})
    assert r.status_code == 202 and r.json()["status"] == "accepted"
    event_id = r.json()["id"]

    rec = (await client.get(f"/apps/{app_id}/events/{event_id}")).json()
    assert rec["type"] == "t"
    assert [d["url"] for d in rec["deliveries"]] == ["http://r/hook/a", "http://r/hook/b"]
    assert all(d["status"] == "pending" and d["attempts"] == 0 for d in rec["deliveries"])

    async with pool.connection() as conn:
        n = await (await conn.execute("SELECT count(*) AS n FROM deliveries")).fetchone()
    assert n["n"] == 2


async def test_event_with_no_endpoints_is_still_accepted(client):
    app_id = (await client.post("/apps", json={"name": "shop"})).json()["id"]
    r = await client.post(f"/apps/{app_id}/events", json={"type": "t", "payload": {}})
    assert r.status_code == 202
    rec = (await client.get(f"/apps/{app_id}/events/{r.json()['id']}")).json()
    assert rec["deliveries"] == []


async def test_unknown_event_is_404(client):
    app_id = (await client.post("/apps", json={"name": "shop"})).json()["id"]
    r = await client.get(f"/apps/{app_id}/events/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 404
