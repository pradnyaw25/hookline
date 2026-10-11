"""The delivery worker. Polls the deliveries table and sends.

    make worker            # or: python -m hookline.worker

Stage 2: claim a batch atomically under a lease (ADR-0002), POST each one, record the
result. Several of these can run side by side; a dead one's rows come back after its lease.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import time
from typing import Any

import httpx
from psycopg import AsyncConnection

from hookline import db

log = logging.getLogger("hookline.worker")
WORKER_ID = f"{socket.gethostname().split('.')[0]}-{os.getpid()}"


async def claim(conn: AsyncConnection, batch: int, lease_s: float) -> list[dict[str, Any]]:
    """Take up to ``batch`` deliveries nobody else holds and mark them ours for ``lease_s``.

    One statement, so two workers can never take the same row: ``FOR UPDATE`` locks what we
    select, ``SKIP LOCKED`` steps over rows another worker is selecting right now, and a
    ``delivering`` row whose lease has expired is treated as abandoned and taken too.
    """
    rows = await (
        await conn.execute(
            """
            UPDATE deliveries
               SET status = 'delivering', attempts = attempts + 1,
                   locked_by = %(worker)s, locked_until = now() + %(lease)s * interval '1 second',
                   updated_at = now()
             WHERE id IN (
                   SELECT id FROM deliveries
                    WHERE status = 'pending'
                       OR (status = 'delivering' AND locked_until < now())
                    ORDER BY id
                    LIMIT %(batch)s
                    FOR UPDATE SKIP LOCKED)
            RETURNING id
            """,
            {"worker": WORKER_ID, "lease": lease_s, "batch": batch},
        )
    ).fetchall()
    ids = [r["id"] for r in rows]
    if not ids:
        return []
    return await (
        await conn.execute(
            "SELECT d.id, d.event_id, e.url, ev.type, ev.payload"
            " FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id"
            " JOIN events ev ON ev.id = d.event_id WHERE d.id = ANY(%s) ORDER BY d.id",
            (ids,),
        )
    ).fetchall()


async def send(http: httpx.AsyncClient, d: dict[str, Any]) -> tuple[bool, int | None, str | None]:
    headers = {
        "Webhook-Id": str(d["event_id"]),
        "Webhook-Timestamp": str(int(time.time())),
        "X-Hookline-Event-Type": d["type"],
    }
    try:
        r = await http.post(d["url"], json=d["payload"], headers=headers)
        return r.is_success, r.status_code, None
    except httpx.HTTPError as e:
        return False, None, f"{type(e).__name__}: {e}"


async def deliver_one(conn: AsyncConnection, http: httpx.AsyncClient, d: dict[str, Any]) -> None:
    t0 = time.perf_counter()
    ok, status, error = await send(http, d)
    await conn.execute(
        "UPDATE deliveries SET status = %s, last_status = %s, last_error = %s,"
        " locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id = %s",
        ("delivered" if ok else "failed", status, error, d["id"]),
    )
    log.info(
        "delivery=%s event=%s %s status=%s %.0fms",
        d["id"],
        d["event_id"],
        "delivered" if ok else "FAILED",
        status if status is not None else error,
        (time.perf_counter() - t0) * 1000,
    )


async def run(stop: asyncio.Event) -> None:
    s = db.settings
    log.info(
        "worker %s starting: batch=%d poll=%.2fs lease=%.0fs",
        WORKER_ID,
        s.worker_batch,
        s.worker_poll_s,
        s.worker_lease_s,
    )
    async with (
        await AsyncConnection.connect(
            s.database_url, autocommit=True, row_factory=db.dict_row
        ) as conn,
        httpx.AsyncClient(timeout=s.delivery_timeout_s) as http,
    ):
        while not stop.is_set():
            batch = await claim(conn, s.worker_batch, s.worker_lease_s)
            if not batch:
                await asyncio.sleep(s.worker_poll_s)
                continue
            log.info("claimed %d: %s", len(batch), [d["id"] for d in batch])
            await asyncio.gather(*(deliver_one(conn, http, d) for d in batch))
    log.info("worker %s stopped", WORKER_ID)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format=f"%(asctime)s {WORKER_ID} %(message)s", datefmt="%H:%M:%S"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    async def _main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await run(stop)

    asyncio.run(_main())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
