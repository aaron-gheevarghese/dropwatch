"""Shared fixtures for DB-dependent tests.

There's no local Postgres available in this project — models use Postgres-specific
UUID/Numeric types, and the "concurrent duplicate polls" scenario in particular needs
real multi-connection transaction semantics that SQLite can't faithfully provide anyway.
So DB-dependent tests run against the real (dev) Supabase database, wrapped in an outer
transaction that's rolled back after every test — nothing a test does is ever actually
committed. See https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#joining-a-session-into-an-external-transaction-such-as-for-test-suites.

The one exception is tests/test_concurrent_idempotency.py, which needs two independent
connections committing concurrently to exercise a real race — that can't use this
rolled-back-savepoint fixture (a single outer connection/transaction can't be shared
across concurrent commits) and instead cleans up its own throwaway rows explicitly.
"""

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from db.client import engine


@pytest_asyncio.fixture
async def db_session():
    async with engine.connect() as connection:
        await connection.begin()
        session = AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint")
        try:
            yield session
        finally:
            await session.close()
            await connection.rollback()
