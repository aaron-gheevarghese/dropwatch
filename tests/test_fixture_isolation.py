"""Proves the db_session rollback fixture (tests/conftest.py) is airtight — not just
"should be fine" by reasoning about join_transaction_mode="create_savepoint", but
empirically: a row written through it must never be visible to an independent
connection, including WHILE the fixture's transaction is still open (before teardown
runs), and even when the test body raises partway through. This is the one property
Steps 1-4's DB-dependent tests all depend on to be safe to run against the real dev DB.
"""

from sqlalchemy import select

from db.client import async_session_factory
from db.models import Pair


async def test_uncommitted_row_is_invisible_to_an_independent_connection(db_session) -> None:
    pair = Pair(
        kraken_pair_name="TESTMVCCISOLATIONUSD",
        display_name="TEST/USD",
        base_currency="TEST",
        quote_currency="USD",
        poll_interval_seconds=60,
        is_active=False,
    )
    db_session.add(pair)
    await db_session.flush()  # sent to Postgres, but only inside db_session's own transaction

    # A different connection entirely, not just a different query on the same one.
    async with async_session_factory() as other_session:
        visible = await other_session.scalar(select(Pair).where(Pair.kraken_pair_name == "TESTMVCCISOLATIONUSD"))
        assert visible is None, "a row inside the rollback fixture's transaction leaked to another connection"


async def test_row_stays_invisible_even_if_the_test_body_raises() -> None:
    # Can't use the db_session fixture for this one — we need to control teardown
    # ourselves to prove the *sequence* (crash, then rollback, then still invisible),
    # not just delegate to pytest's fixture machinery and hope. Mirrors conftest.py's
    # db_session fixture exactly.
    from sqlalchemy.ext.asyncio import AsyncSession

    from db.client import engine

    async with engine.connect() as connection:
        await connection.begin()
        session = AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint")
        try:
            pair = Pair(
                kraken_pair_name="TESTCRASHISOLATIONUSD",
                display_name="TEST/USD",
                base_currency="TEST",
                quote_currency="USD",
                poll_interval_seconds=60,
                is_active=False,
            )
            session.add(pair)
            await session.flush()
            raise RuntimeError("simulated crash mid-test")
        except RuntimeError:
            pass
        finally:
            await session.close()
            await connection.rollback()

    async with async_session_factory() as check_session:
        visible = await check_session.scalar(
            select(Pair).where(Pair.kraken_pair_name == "TESTCRASHISOLATIONUSD")
        )
        assert visible is None
