"""Unit tests for scraper/db.py — async asyncpg pool singleton."""
import os
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Reset module-level _pool between tests
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def reset_pool():
    import db
    db._pool = None
    yield
    db._pool = None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_get_pool_creates_pool_on_first_call(mocker):
    mock_pool = MagicMock()
    mock_create = mocker.patch(
        "asyncpg.create_pool",
        new_callable=AsyncMock,
        return_value=mock_pool,
    )
    import db

    pool = await db.get_pool()

    mock_create.assert_awaited_once()
    assert pool is mock_pool


@pytest.mark.asyncio
async def test_get_pool_returns_cached_instance_on_second_call(mocker):
    mock_pool = MagicMock()
    mock_create = mocker.patch(
        "asyncpg.create_pool",
        new_callable=AsyncMock,
        return_value=mock_pool,
    )
    import db

    p1 = await db.get_pool()
    p2 = await db.get_pool()

    assert p1 is p2
    assert mock_create.await_count == 1


@pytest.mark.asyncio
async def test_get_pool_uses_database_url_env(mocker):
    custom_dsn = "postgresql://user:pass@myhost:5433/mydb"
    mocker.patch.dict(os.environ, {"DATABASE_URL": custom_dsn})
    mock_create = mocker.patch(
        "asyncpg.create_pool",
        new_callable=AsyncMock,
        return_value=MagicMock(),
    )
    import db

    await db.get_pool()

    call_kwargs = mock_create.call_args[1]
    assert call_kwargs["dsn"] == custom_dsn


@pytest.mark.asyncio
async def test_get_pool_uses_default_dsn_when_no_env(mocker):
    # Remove DATABASE_URL from env so the default DSN is used
    env_without_db_url = {
        k: v for k, v in os.environ.items() if k != "DATABASE_URL"
    }
    mocker.patch.dict(os.environ, env_without_db_url, clear=True)
    mock_create = mocker.patch(
        "asyncpg.create_pool",
        new_callable=AsyncMock,
        return_value=MagicMock(),
    )
    import db

    await db.get_pool()

    call_kwargs = mock_create.call_args[1]
    assert call_kwargs["dsn"] == "postgresql://cartracker@postgres:5432/cartracker"


@pytest.mark.asyncio
async def test_get_pool_passes_min_max_size(mocker):
    mock_create = mocker.patch(
        "asyncpg.create_pool",
        new_callable=AsyncMock,
        return_value=MagicMock(),
    )
    import db

    await db.get_pool()

    call_kwargs = mock_create.call_args[1]
    assert call_kwargs["min_size"] == 1
    assert call_kwargs["max_size"] == 5


@pytest.mark.asyncio
async def test_close_pool_closes_and_resets(mocker):
    mock_pool = AsyncMock()
    mock_pool.close = AsyncMock()
    mocker.patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool)
    import db

    await db.get_pool()
    await db.close_pool()

    mock_pool.close.assert_awaited_once()
    assert db._pool is None


@pytest.mark.asyncio
async def test_close_pool_noop_if_no_pool():
    import db
    # _pool is already None (reset_pool fixture); should not raise
    await db.close_pool()
