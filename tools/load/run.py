"""Load script: push events into hookline and measure what comes out the other side.

    make load ARGS="--events 500 --concurrency 20"

It talks to two things: the service (your code) to create an app, endpoints and events, and
the flaky receiver to learn what was actually delivered. Every event carries ``sent_at`` so the
receiver can compute end-to-end latency without the two sides sharing any state.

Before each run it zeroes the receiver's counters but keeps whatever behavior you configured,
so the flow is: shape the receiver, run the load script, read the report.

The API contract it expects is the one in docs/stages/01-synchronous-delivery.md.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any

import httpx


@dataclass
class Result:
    expected: int
    ingest_latency_s: list[float] = field(default_factory=list)
    ingest_status: dict[str, int] = field(default_factory=dict)
    ingest_errors: int = 0
    ingest_wall_s: float = 0.0
    wait_wall_s: float = 0.0
    delivered_in_time: bool = False
    receiver: dict[str, Any] = field(default_factory=dict)


def pct(xs: list[float], p: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    return s[min(len(s) - 1, int(p * len(s)))]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--receiver", default="http://localhost:9000")
    ap.add_argument(
        "--hook", default="load", help="receiver hook name; endpoints point at /hook/<hook>-<i>"
    )
    ap.add_argument("--events", type=int, default=100)
    ap.add_argument("--concurrency", type=int, default=10, help="max in-flight event POSTs")
    ap.add_argument("--endpoints", type=int, default=1, help="subscribers per app")
    ap.add_argument("--payload-bytes", type=int, default=256)
    ap.add_argument("--event-type", default="load.test")
    ap.add_argument(
        "--timeout", type=float, default=30.0, help="per-request timeout against the API"
    )
    ap.add_argument(
        "--wait", type=float, default=60.0, help="seconds to wait for deliveries after ingest"
    )
    ap.add_argument(
        "--no-reset", action="store_true", help="keep receiver counters from earlier runs"
    )
    ap.add_argument("--json", action="store_true", help="print the raw result as JSON")
    return ap.parse_args(argv)


async def setup(
    api: httpx.AsyncClient, rcv: httpx.AsyncClient, a: argparse.Namespace
) -> tuple[str, list[str]]:
    hook_names = [f"{a.hook}-{i}" for i in range(a.endpoints)]
    if not a.no_reset:
        for name in hook_names:
            (await rcv.post(f"/control/{name}/reset-stats")).raise_for_status()
    r = await api.post("/apps", json={"name": f"load-{int(time.time())}"})
    r.raise_for_status()
    app_id = r.json()["id"]
    endpoint_ids = []
    for name in hook_names:
        r = await api.post(f"/apps/{app_id}/endpoints", json={"url": f"{a.receiver}/hook/{name}"})
        r.raise_for_status()
        endpoint_ids.append(r.json()["id"])
    return app_id, hook_names


async def fire(api: httpx.AsyncClient, a: argparse.Namespace, app_id: str, res: Result) -> None:
    sem = asyncio.Semaphore(a.concurrency)
    pad = "x" * max(0, a.payload_bytes - 64)

    async def one(i: int) -> None:
        async with sem:
            t0 = time.perf_counter()
            try:
                r = await api.post(
                    f"/apps/{app_id}/events",
                    json={
                        "type": a.event_type,
                        "payload": {"n": i, "sent_at": time.time(), "pad": pad},
                    },
                )
                key = str(r.status_code)
            except httpx.HTTPError as e:
                key = type(e).__name__
                res.ingest_errors += 1
            res.ingest_latency_s.append(time.perf_counter() - t0)
            res.ingest_status[key] = res.ingest_status.get(key, 0) + 1

    t0 = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(a.events)))
    res.ingest_wall_s = time.perf_counter() - t0


async def wait_for_deliveries(
    rcv: httpx.AsyncClient, hook_names: list[str], a: argparse.Namespace, res: Result
) -> None:
    t0 = time.perf_counter()
    deadline = t0 + a.wait
    while True:
        stats = [(await rcv.get(f"/control/{n}/stats")).json() for n in hook_names]
        got = sum(s["unique_events_succeeded"] for s in stats)
        if got >= res.expected:
            res.delivered_in_time = True
            break
        if time.perf_counter() > deadline:
            break
        await asyncio.sleep(0.5)
    res.wait_wall_s = time.perf_counter() - t0
    res.receiver = merge_stats(stats)


def merge_stats(stats: list[dict[str, Any]]) -> dict[str, Any]:
    """Sum the counters across hooks; latency percentiles are taken from the worst hook."""
    out: dict[str, Any] = {}
    for s in stats:
        for k, v in s.items():
            if isinstance(v, int | float) and not k.endswith("_at"):
                out[k] = out.get(k, 0) + v
    lat = [s["latency_s"] for s in stats if s["latency_s"]["n"]]
    out["latency_s"] = max(lat, key=lambda x: x["p99"] or 0) if lat else {"n": 0}
    out["max_in_flight"] = max((s["max_in_flight"] for s in stats), default=0)
    out["attempts_per_event_max"] = max((s["attempts_per_event_max"] for s in stats), default=0)
    return out


def _lat(xs: list[float] | None, stats: dict[str, Any] | None = None) -> str:
    if stats is not None:
        if not stats.get("n"):
            return "(none)"
        p50, p95, p99, mx = stats["p50"], stats["p95"], stats["p99"], stats["max"]
    elif xs:
        p50, p95, p99, mx = pct(xs, 0.5), pct(xs, 0.95), pct(xs, 0.99), max(xs)
    else:
        return "(none)"
    return f"p50 {p50:.3f}s  p95 {p95:.3f}s  p99 {p99:.3f}s  max {mx:.3f}s"


def report(res: Result, a: argparse.Namespace) -> str:
    r = res.receiver
    delivered = r.get("unique_events_succeeded", 0)
    throughput = a.events / res.ingest_wall_s if res.ingest_wall_s else 0
    verdict = "ok" if res.delivered_in_time else f"TIMED OUT after {a.wait:.0f}s"
    lines = [
        f"events={a.events} endpoints={a.endpoints} concurrency={a.concurrency} "
        f"payload={a.payload_bytes}B",
        "",
        "INGEST  (producer → service)",
        f"  wall        {res.ingest_wall_s:8.2f}s   throughput {throughput:8.1f} events/s",
        f"  latency     {_lat(res.ingest_latency_s)}",
        f"  status      {res.ingest_status}",
        "",
        "DELIVERY (service → receiver)",
        f"  delivered   {delivered}/{res.expected}   pending {res.expected - delivered}  {verdict}",
        f"  attempts    {r.get('total_requests', 0)} total, "
        f"max {r.get('attempts_per_event_max', 0)} per event, "
        f"{r.get('duplicates', 0)} duplicate deliveries",
        f"  failed      {r.get('failed', 0)} failed, {r.get('rejected_down', 0)} down, "
        f"{r.get('rejected_concurrency', 0)} over-concurrency, "
        f"{r.get('bad_signature', 0)} bad signature, {r.get('hung', 0)} hung",
        f"  in-flight   max {r.get('max_in_flight', 0)} at the receiver",
        f"  latency     {_lat(None, r.get('latency_s', {}))}   (sent_at → first 2xx)",
    ]
    return "\n".join(lines)


async def run(a: argparse.Namespace) -> Result:
    res = Result(expected=a.events * a.endpoints)
    limits = httpx.Limits(max_connections=a.concurrency + 5)
    async with (
        httpx.AsyncClient(base_url=a.api, timeout=a.timeout, limits=limits) as api,
        httpx.AsyncClient(base_url=a.receiver, timeout=10) as rcv,
    ):
        app_id, hook_names = await setup(api, rcv, a)
        await fire(api, a, app_id, res)
        await wait_for_deliveries(rcv, hook_names, a, res)
    return res


def main(argv: list[str] | None = None) -> int:
    a = parse_args(argv)
    res = asyncio.run(run(a))
    if a.json:
        print(json.dumps(res.__dict__, default=str, indent=2))
    else:
        print(report(res, a))
    return 0 if res.delivered_in_time and not res.ingest_errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
