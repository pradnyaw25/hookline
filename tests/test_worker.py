"""Stage 2: workers claim rows exclusively under a lease, send, and record the outcome."""

import asyncio

import httpx
import pytest
from psycopg import AsyncConnection

from hookline import db, worker
from hookline.main import app
from tools.receiver import app as receiver


@pytest.fixture
async def env(pool, database_url):
    """API client, worker connection, and an http client wired to the in-process receiver."""
    receiver.hooks.clear()
    app.state.pool = pool
    async with (
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://s") as api,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=receiver.app), base_url="http://r", timeout=5
        ) as http,
        await AsyncConnection.connect(
            database_url, autocommit=True, row_factory=db.dict_row
        ) as conn,
    ):
        yield api, conn, http


async def seed(api: httpx.AsyncClient, hooks: list[str], events: int = 1) -> tuple[str, list[str]]:
    app_id = (await api.post("/apps", json={"name": "shop"})).json()["id"]
    for h in hooks:
        await api.post(f"/apps/{app_id}/endpoints", json={"url": f"http://r/hook/{h}"})
    ids = []
    for i in range(events):
        r = await api.post(f"/apps/{app_id}/events", json={"type": "t", "payload": {"n": i}})
        ids.append(r.json()["id"])
    return app_id, ids


async def test_claim_then_deliver_marks_delivered(env):
    api, conn, http = env
    app_id, (event_id,) = await seed(api, ["a", "b"])
    batch = await worker.claim(conn, 10, 30)
    assert len(batch) == 2 and all(str(d["event_id"]) == event_id for d in batch)
    for d in batch:
        await worker.deliver_one(conn, http, d)
    rec = (await api.get(f"/apps/{app_id}/events/{event_id}")).json()
    assert all(d["status"] == "delivered" and d["attempts"] == 1 for d in rec["deliveries"])
    assert all(d["last_status"] == 200 for d in rec["deliveries"])
    for h in ("a", "b"):
        assert event_id in receiver.hooks[h].succeeded_events, "Webhook-Id is the event id"
    assert await worker.claim(conn, 10, 30) == [], "nothing left to claim"


async def test_claim_honours_batch_and_order(env):
    api, conn, http = env
    _, ids = await seed(api, ["a"], events=5)
    first = await worker.claim(conn, 2, 30)
    second = await worker.claim(conn, 10, 30)
    assert [str(d["event_id"]) for d in first] == ids[:2]
    assert [str(d["event_id"]) for d in second] == ids[2:]


async def test_failed_delivery_is_recorded_not_retried(env):
    api, conn, http = env
    receiver.hooks["bad"].behavior = receiver.Behavior(down=True)
    app_id, (event_id,) = await seed(api, ["bad"])
    for d in await worker.claim(conn, 10, 30):
        await worker.deliver_one(conn, http, d)
    (d,) = (await api.get(f"/apps/{app_id}/events/{event_id}")).json()["deliveries"]
    assert d["status"] == "failed" and d["last_status"] == 503 and d["attempts"] == 1
    assert await worker.claim(conn, 10, 30) == [], "stage 2 does not retry"


async def test_two_workers_never_claim_the_same_row(env, database_url):
    """Break 1 of docs/stages/02-postgres-queue.md, in miniature."""
    api, conn, http = env
    await seed(api, ["a"], events=40)
    async with await AsyncConnection.connect(
        database_url, autocommit=True, row_factory=db.dict_row
    ) as conn2:
        results = await asyncio.gather(
            *(worker.claim(c, 5, 30) for c in (conn, conn2, conn, conn2) for _ in range(4))
        )
    claimed = [d["id"] for batch in results for d in batch]
    assert len(claimed) == 40 and len(set(claimed)) == 40


async def test_expired_lease_is_reclaimed_with_attempts_counted(env):
    """Break 2: a worker dies holding rows. After the lease, another worker takes them."""
    api, conn, http = env
    app_id, (event_id,) = await seed(api, ["a"])
    (held,) = await worker.claim(conn, 10, lease_s=0.2)
    assert await worker.claim(conn, 10, 30) == [], "a live lease is respected"
    await asyncio.sleep(0.3)
    (reclaimed,) = await worker.claim(conn, 10, 30)
    assert reclaimed["id"] == held["id"]
    await worker.deliver_one(conn, http, reclaimed)
    (d,) = (await api.get(f"/apps/{app_id}/events/{event_id}")).json()["deliveries"]
    assert d["status"] == "delivered" and d["attempts"] == 2
    row = await (
        await conn.execute(
            "SELECT locked_by, locked_until FROM deliveries WHERE id = %s", (d["id"],)
        )
    ).fetchone()
    assert row == {"locked_by": None, "locked_until": None}, "a finished row holds no lease"
