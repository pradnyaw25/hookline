"""The flaky receiver is the instrument every stage is measured with, so it gets tested."""

import asyncio
import json
import time

import httpx
import pytest

from tools.receiver import app as receiver
from tools.receiver.signing import sign


@pytest.fixture
async def client():
    receiver.hooks.clear()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=receiver.app), base_url="http://r"
    ) as c:
        yield c


def payload(n: int = 0) -> dict:
    return {"n": n, "sent_at": time.time()}


async def test_default_hook_is_healthy(client):
    r = await client.post("/hook/a", json=payload())
    assert r.status_code == 200
    s = (await client.get("/control/a/stats")).json()
    assert s["succeeded"] == 1 and s["failed"] == 0 and s["latency_s"]["n"] == 1


async def test_fail_attempts_is_per_event_id(client):
    await client.put("/control/a", json={"fail_attempts": 2, "status_code": 503})
    for expected in (503, 503, 200):
        r = await client.post("/hook/a", json=payload(), headers={"Webhook-Id": "evt_1"})
        assert r.status_code == expected
    # a different event starts its own count
    r = await client.post("/hook/a", json=payload(), headers={"Webhook-Id": "evt_2"})
    assert r.status_code == 503
    s = (await client.get("/control/a/stats")).json()
    assert s["attempts_per_event_max"] == 3 and s["unique_events_succeeded"] == 1


async def test_duplicates_are_counted_not_rejected(client):
    for _ in range(3):
        r = await client.post("/hook/a", json=payload(), headers={"Webhook-Id": "evt_1"})
        assert r.status_code == 200
    s = (await client.get("/control/a/stats")).json()
    assert s["succeeded"] == 3 and s["duplicates"] == 2 and s["unique_events_succeeded"] == 1
    assert s["latency_s"]["n"] == 1, "latency is recorded once per event, on its first success"


async def test_down_returns_503_without_recording_body(client):
    await client.put("/control/a", json={"down": True})
    r = await client.post("/hook/a", json=payload())
    assert r.status_code == 503
    s = (await client.get("/control/a/stats")).json()
    assert s["rejected_down"] == 1 and s["total_requests"] == 1


async def test_max_concurrent_sheds_with_429(client):
    await client.put("/control/a", json={"max_concurrent": 2, "delay_ms": 200})
    results = await asyncio.gather(*(client.post("/hook/a", json=payload(i)) for i in range(5)))
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 200, 429, 429, 429]
    s = (await client.get("/control/a/stats")).json()
    assert s["max_in_flight"] == 2 and s["rejected_concurrency"] == 3


async def test_signature_verification(client):
    secret = "whsec_" + "c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0"
    await client.put("/control/a", json={"secret": secret})
    body = json.dumps(payload()).encode()
    ts = int(time.time())
    good = {
        "Webhook-Id": "evt_1",
        "Webhook-Timestamp": str(ts),
        "Webhook-Signature": sign(secret, "evt_1", ts, body),
    }
    assert (await client.post("/hook/a", content=body, headers=good)).status_code == 200

    bad = {**good, "Webhook-Signature": "v1,AAAA"}
    r = await client.post("/hook/a", content=body, headers=bad)
    assert r.status_code == 401 and r.text == "no matching signature"

    stale = {
        **good,
        "Webhook-Timestamp": str(ts - 3600),
        "Webhook-Signature": sign(secret, "evt_1", ts - 3600, body),
    }
    assert (await client.post("/hook/a", content=body, headers=stale)).status_code == 401

    rotated = {**good, "Webhook-Signature": "v1,AAAA " + good["Webhook-Signature"]}
    assert (await client.post("/hook/a", content=body, headers=rotated)).status_code == 200, (
        "overlap during rotation"
    )
    assert (await client.get("/control/a/stats")).json()["bad_signature"] == 2


async def test_reset_stats_keeps_behavior(client):
    await client.put("/control/a", json={"delay_ms": 5})
    await client.post("/hook/a", json=payload())
    await client.post("/control/a/reset-stats")
    s = (await client.get("/control/a")).json()
    assert s["stats"]["total_requests"] == 0 and s["behavior"]["delay_ms"] == 5


async def test_patch_merges_and_delete_resets(client):
    await client.put("/control/a", json={"delay_ms": 50})
    await client.patch("/control/a", json={"fail_rate": 1.0})
    b = (await client.get("/control/a")).json()["behavior"]
    assert b["delay_ms"] == 50 and b["fail_rate"] == 1.0
    await client.post("/hook/a", json=payload())
    await client.delete("/control/a")
    s = (await client.get("/control/a")).json()
    assert s["stats"]["total_requests"] == 0 and s["behavior"]["delay_ms"] == 0


async def test_received_ring_keeps_recent_requests(client):
    for i in range(3):
        await client.post("/hook/a", json=payload(i), headers={"Webhook-Id": f"evt_{i}"})
    recent = (await client.get("/control/a/received", params={"limit": 2})).json()
    assert [r["event_id"] for r in recent] == ["evt_1", "evt_2"]
    assert recent[0]["body"]["n"] == 1
