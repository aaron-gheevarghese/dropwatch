"""POST /rules/backtest: locks in two real bugs found and fixed while testing this
against real accumulated PriceHistory data.

1. notifications.rule_id is a real foreign key against alert_rules.id. A temp rule
   that's constructed but never added to the session fails that FK on every single
   fire (silently — caught by the same IntegrityError handler evaluate_rules_for_pair
   uses for the concurrent-race case), producing a false fire_count=0 regardless of
   the rule. The fix is to add()/flush() the temp rule for real (satisfying the FK
   within the transaction) and rely on never committing for "no persistence" — not on
   skipping the insert.
2. Notification.triggered_at is server_default=func.now() — Postgres's now() is the
   TRANSACTION timestamp, constant across every statement in one bulk backtest
   transaction. Reading it back per-fire reports the same wall-clock instant for every
   fire regardless of which historical moment actually triggered it. The endpoint
   tracks each fire's real historical timestamp (the replayed row's observed_at)
   itself instead of trusting the DB-generated column.

Needs real commits, not the db_session rollback fixture: backtest_rule opens its own
session via async_session_factory(), a different connection than any test fixture's
session, so it can only see data another session actually committed. Cleaned up
explicitly in a finally block, same pattern as test_idempotency_redelivery.py's
concurrent-race test.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import select

from api.routes.rules import BacktestRequest, backtest_rule
from db.client import async_session_factory
from db.models import AlertRule, Notification, Pair, PriceHistory, User


async def _cleanup(pair_id) -> None:
    async with async_session_factory() as session:
        pair = await session.get(Pair, pair_id)
        if pair is not None:
            await session.delete(pair)  # cascades: alert_rules -> notifications -> outbox_events
            await session.commit()


async def test_backtest_finds_real_fires_with_correct_distinct_timestamps() -> None:
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)
    # 5 observations, 3 of them below the threshold, each a minute apart.
    prices = [Decimal(p) for p in ("100", "90", "95", "89", "101")]
    expected_fire_indices = [1, 3]  # 90 and 89 are below 92

    async with async_session_factory() as session:
        pair = Pair(
            kraken_pair_name=f"TESTBACKTEST{uuid4().hex[:8].upper()}USD",
            display_name="TEST/USD", base_currency="TEST", quote_currency="USD",
            poll_interval_seconds=60, is_active=False,
        )
        session.add(pair)
        await session.flush()
        for i, price in enumerate(prices):
            session.add(
                PriceHistory(
                    pair_id=pair.id, last_price=price, bid_price=price, ask_price=price,
                    observed_at=base + timedelta(minutes=i),
                )
            )
        await session.commit()
        pair_id = pair.id

    try:
        response = await backtest_rule(
            BacktestRequest(
                pair_id=pair_id, rule_type="absolute_below", threshold=Decimal("92"),
                cooldown_seconds=0, lookback_days=30,
            )
        )

        assert response.fire_count == 2
        assert response.post_cooldown_fire_count == 2
        assert response.observations_replayed == 5

        fired_at = sorted(f.triggered_at for f in response.fires)
        expected_at = sorted(base + timedelta(minutes=i) for i in expected_fire_indices)
        # Regression guard for bug #2: these must be genuinely distinct, not all equal
        # to whatever instant the request happened to run at.
        assert fired_at == expected_at
        assert len({f.triggered_at for f in response.fires}) == 2

        prices_fired = sorted(f.detected_price for f in response.fires)
        assert prices_fired == [Decimal("89"), Decimal("90")]

        # Regression guard for bug #1: the fires must have actually been found at all.
        assert response.fire_count > 0
    finally:
        await _cleanup(pair_id)


async def test_backtest_persists_nothing(monkeypatch) -> None:
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)

    async with async_session_factory() as session:
        pair = Pair(
            kraken_pair_name=f"TESTBACKTESTNOPERSIST{uuid4().hex[:6].upper()}USD",
            display_name="TEST/USD", base_currency="TEST", quote_currency="USD",
            poll_interval_seconds=60, is_active=False,
        )
        session.add(pair)
        await session.flush()
        for i in range(5):
            price = Decimal("50")  # always fires against a threshold of 1000
            session.add(
                PriceHistory(
                    pair_id=pair.id, last_price=price, bid_price=price, ask_price=price,
                    observed_at=base + timedelta(minutes=i),
                )
            )
        await session.commit()
        pair_id = pair.id

    try:
        response = await backtest_rule(
            BacktestRequest(
                pair_id=pair_id, rule_type="absolute_below", threshold=Decimal("1000"),
                cooldown_seconds=0, lookback_days=30,
            )
        )
        assert response.fire_count == 5  # confirms it actually ran, not a vacuous pass below

        async with async_session_factory() as session:
            rule_count = len((await session.execute(select(AlertRule).where(AlertRule.pair_id == pair_id))).all())
            notification_count = len(
                (await session.execute(select(Notification).where(Notification.pair_id == pair_id))).all()
            )
            assert rule_count == 0, "the temp rule must never survive the backtest"
            assert notification_count == 0, "no Notification may survive a backtest run"
    finally:
        await _cleanup(pair_id)


async def test_backtest_unknown_pair_404s() -> None:
    try:
        await backtest_rule(
            BacktestRequest(
                pair_id=uuid4(), rule_type="absolute_below", threshold=Decimal("1"),
                cooldown_seconds=0, lookback_days=30,
            )
        )
        raise AssertionError("expected HTTPException")
    except HTTPException as exc:
        assert exc.status_code == 404


async def test_backtest_cooldown_collapses_post_cooldown_count() -> None:
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)
    prices = [Decimal(p) for p in ("80", "79", "78", "77", "76")]  # 5 consecutive fires, 1 min apart

    async with async_session_factory() as session:
        pair = Pair(
            kraken_pair_name=f"TESTBACKTESTCD{uuid4().hex[:6].upper()}USD",
            display_name="TEST/USD", base_currency="TEST", quote_currency="USD",
            poll_interval_seconds=60, is_active=False,
        )
        session.add(pair)
        await session.flush()
        for i, price in enumerate(prices):
            session.add(
                PriceHistory(
                    pair_id=pair.id, last_price=price, bid_price=price, ask_price=price,
                    observed_at=base + timedelta(minutes=i),
                )
            )
        await session.commit()
        pair_id = pair.id

    try:
        response = await backtest_rule(
            BacktestRequest(
                pair_id=pair_id, rule_type="absolute_below", threshold=Decimal("100"),
                cooldown_seconds=3600, lookback_days=30,  # 1hr cooldown, all 5 fires within 5 minutes
            )
        )
        assert response.fire_count == 5
        assert response.post_cooldown_fire_count == 1
    finally:
        await _cleanup(pair_id)
