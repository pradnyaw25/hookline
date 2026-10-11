"""Settings, the connection pool, and a migration runner small enough to read.

    python -m hookline.db migrate        # apply src/hookline/migrations/*.sql in order

Migrations are plain SQL files named ``NNNN_name.sql``; each is applied in its own
transaction and recorded in ``schema_migrations``. Raw SQL throughout: the queries *are* the
course material, so nothing should hide them.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic_settings import BaseSettings, SettingsConfigDict

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://hookline:hookline@localhost:5432/hookline"
    # How long we wait for a subscriber to answer a single POST.
    delivery_timeout_s: float = 60.0
    # Worker: rows claimed per poll, and how long to sleep when there is nothing to do.
    worker_batch: int = 10
    worker_poll_s: float = 0.5
    # How long a claim is ours. Must exceed delivery_timeout_s plus margin (ADR-0002).
    worker_lease_s: float = 90.0


settings = Settings()


def make_pool(url: str | None = None, max_size: int = 10) -> AsyncConnectionPool:
    return AsyncConnectionPool(
        url or settings.database_url,
        open=False,
        min_size=1,
        max_size=max_size,
        kwargs={"row_factory": dict_row},
    )


def _migration_files() -> list[tuple[int, Path]]:
    out = []
    for p in sorted(MIGRATIONS_DIR.glob("*.sql")):
        m = re.match(r"^(\d+)_", p.name)
        if m:
            out.append((int(m.group(1)), p))
    return out


def migrate(url: str | None = None) -> list[int]:
    """Apply unapplied migrations; return the versions applied. Sync on purpose: it's a CLI."""
    applied: list[int] = []
    with psycopg.connect(url or settings.database_url) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        conn.commit()
        done = {r[0] for r in conn.execute("SELECT version FROM schema_migrations")}
        for version, path in _migration_files():
            if version in done:
                continue
            with conn.transaction():
                conn.execute(path.read_text())
                conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
            applied.append(version)
    return applied


def drop_all(url: str | None = None) -> None:
    """Tests only: wipe the schema so migrate() starts from nothing."""
    with psycopg.connect(url or settings.database_url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")


def main(argv: list[str]) -> int:
    if argv[:1] == ["migrate"]:
        applied = migrate()
        print(f"applied {applied or 'nothing'} to {settings.database_url}")
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
