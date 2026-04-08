"""
Async database connection pool for the scraper admin UI.
"""
import os

import asyncpg

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=os.environ.get("DATABASE_URL", "postgresql://cartracker@postgres:5432/cartracker"),
            min_size=1,
            max_size=5,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
