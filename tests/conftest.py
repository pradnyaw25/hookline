"""Service tests run against a real Postgres (``hookline_test``), created on first use.

Set TEST_DATABASE_URL to point elsewhere. If the server is unreachable the service tests are
skipped, so ``make test`` still exercises the tooling without a database.
"""

from __future__ import annotations

import os

import psycopg
import pytest

from hookline import db

TEST_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://hookline:hookline@localhost:5432/hookline_test"
)


def _ensure_database() -> str | None:
    """Create the test database if missing. Return a skip reason if the server is unreachable."""
    admin = TEST_URL.rsplit("/", 1)[0] + "/hookline"
    name = TEST_URL.rsplit("/", 1)[1]
    try:
        with psycopg.connect(admin, autocommit=True, connect_timeout=2) as conn:
            if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
                conn.execute(f'CREATE DATABASE "{name}"')
    except psycopg.OperationalError as e:
        return f"postgres unreachable at {admin}: {e}".splitlines()[0]
    return None


@pytest.fixture(scope="session")
def database_url() -> str:
    reason = _ensure_database()
    if reason:
        pytest.skip(reason)
    db.drop_all(TEST_URL)
    db.migrate(TEST_URL)
    return TEST_URL


@pytest.fixture
async def pool(database_url):
    """A fresh pool per test (pytest-asyncio gives each test its own loop) over empty tables."""
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute("TRUNCATE deliveries, events, endpoints, apps")
    p = db.make_pool(database_url, max_size=5)
    await p.open()
    try:
        yield p
    finally:
        await p.close()
