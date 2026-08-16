"""End-to-end wiring tests: zscore_move rules evaluated through evaluate_rules_for_pair
against real PriceHistory rows in the DB, exactly as workers/poller.py calls it. Proves
the autoflush-visibility assumption _fetch_recent_prices relies on (the PriceHistory
row the caller just added for the CURRENT observation must be visible to the very next
SELECT in the same transaction, without an explicit flush) and the direction/zero-
variance/insufficient-history behaviors end to end, not just at the pure-function level.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from db.models import AlertRule, Pair, PriceHistory, User
from rules.evaluator import evaluate_rules_for_pair
from workers.rule_index import RuleIndex


async def _make_pair_and_zscore_rule(
    session, *, sigma: str = "2.0", direction: str | None = None
) -> tuple[Pair, AlertRule, RuleIndex]:
    user = User(contact="test@example.com")
    pair = Pair(
        kraken_pair_name=f"TESTZSCORE{uuid4().hex[:8].upper()}USD",
        display_name="TEST/USD",
        base_currency="TEST",
        quote_currency="USD",
        poll_interval_seconds=60,
        is_active=True,
    )
    session.add_all([user, pair])
    await session.flush()

    rule = AlertRule(
        user_id=user.id,
        pair_id=pair.id,
        rule_type="zscore_move",
        sigma=Decimal(sigma),
        direction=direction,
        is_enabled=True,
    )
    session.add(rule)
    await session.flush()

    # replace_with_rules, not rebuild(): rebuild() queries via its own connection and
    # can't see this rule, which only exists inside db_session's uncommitted transaction.
    rule_index = RuleIndex()
    rule_index.replace_with_rules([rule])
    return pair, rule, rule_index


async def _seed_history(session, pair: Pair, prices: list[float], *, start: datetime, step_seconds: int = 60) -> None:
    for i, price in enumerate(prices):
        session.add(
            PriceHistory(
                pair_id=pair.id,
                last_price=Decimal(str(price)),
                bid_price=Decimal(str(price)),
                ask_price=Decimal(str(price)),
                observed_at=start + timedelta(seconds=i * step_seconds),
            )
        )
    await session.flush()


def _calm_baseline(n: int, seed_price: float = 100.0) -> list[float]:
    import random

    rng = random.Random(7)
    prices = [seed_price]
    for _ in range(n - 1):
        prices.append(prices[-1] * (1 + rng.uniform(-0.001, 0.001)))
    return prices


async def test_current_price_is_visible_via_autoflush_and_anomaly_fires(db_session) -> None:
    pair, rule, rule_index = await _make_pair_and_zscore_rule(db_session)
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)
    history = _calm_baseline(40)
    await _seed_history(db_session, pair, history, start=base)

    current_time = base + timedelta(seconds=40 * 60)
    current_price = Decimal(str(history[-1] * 1.05))  # sharp 5% jump vs ~0.1% jitter baseline

    # Mirrors workers/poller.py exactly: add the PriceHistory row, then evaluate — the
    # evaluator must see this row (via autoflush) as the newest price, not miss it.
    db_session.add(
        PriceHistory(
            pair_id=pair.id, last_price=current_price, bid_price=current_price, ask_price=current_price,
            observed_at=current_time,
        )
    )

    created = await evaluate_rules_for_pair(
        db_session, pair, current_price, current_price, current_price, current_time, rule_index
    )
    assert len(created) == 1
    assert created[0].type == "zscore_move"
    assert created[0].detected_price == current_price


async def test_normal_jitter_does_not_fire(db_session) -> None:
    pair, rule, rule_index = await _make_pair_and_zscore_rule(db_session)
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)
    history = _calm_baseline(40)
    await _seed_history(db_session, pair, history, start=base)

    current_time = base + timedelta(seconds=40 * 60)
    # A move well within the baseline's own jitter range.
    current_price = Decimal(str(history[-1] * 1.0002))

    db_session.add(
        PriceHistory(
            pair_id=pair.id, last_price=current_price, bid_price=current_price, ask_price=current_price,
            observed_at=current_time,
        )
    )
    created = await evaluate_rules_for_pair(
        db_session, pair, current_price, current_price, current_price, current_time, rule_index
    )
    assert created == []


async def test_insufficient_history_never_fires_regardless_of_move_size(db_session) -> None:
    pair, rule, rule_index = await _make_pair_and_zscore_rule(db_session)
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)
    history = _calm_baseline(10)  # well under the 30-observation minimum
    await _seed_history(db_session, pair, history, start=base)

    current_time = base + timedelta(seconds=10 * 60)
    current_price = Decimal(str(history[-1] * 2.0))  # a huge move -- should still not fire

    db_session.add(
        PriceHistory(
            pair_id=pair.id, last_price=current_price, bid_price=current_price, ask_price=current_price,
            observed_at=current_time,
        )
    )
    created = await evaluate_rules_for_pair(
        db_session, pair, current_price, current_price, current_price, current_time, rule_index
    )
    assert created == []


async def test_zero_variance_fallback_respects_configured_minimum_percent(db_session) -> None:
    pair, rule, rule_index = await _make_pair_and_zscore_rule(db_session)
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)
    flat_history = [100.0] * 35
    await _seed_history(db_session, pair, flat_history, start=base)

    # Below the configured zscore_zero_variance_min_percent (0.5% default) -- no fire.
    current_time = base + timedelta(seconds=35 * 60)
    small_move_price = Decimal("100.2")
    db_session.add(
        PriceHistory(
            pair_id=pair.id, last_price=small_move_price, bid_price=small_move_price, ask_price=small_move_price,
            observed_at=current_time,
        )
    )
    created_small = await evaluate_rules_for_pair(
        db_session, pair, small_move_price, small_move_price, small_move_price, current_time, rule_index
    )
    assert created_small == []

    # Above the threshold -- fires via the fallback.
    current_time_2 = current_time + timedelta(seconds=60)
    big_move_price = Decimal("102.0")
    db_session.add(
        PriceHistory(
            pair_id=pair.id, last_price=big_move_price, bid_price=big_move_price, ask_price=big_move_price,
            observed_at=current_time_2,
        )
    )
    created_big = await evaluate_rules_for_pair(
        db_session, pair, big_move_price, big_move_price, big_move_price, current_time_2, rule_index
    )
    assert len(created_big) == 1


async def test_direction_up_ignores_a_drop(db_session) -> None:
    pair, rule, rule_index = await _make_pair_and_zscore_rule(db_session, direction="up")
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)
    history = _calm_baseline(40)
    await _seed_history(db_session, pair, history, start=base)

    current_time = base + timedelta(seconds=40 * 60)
    current_price = Decimal(str(history[-1] * 0.95))  # sharp DROP; rule only cares about "up"

    db_session.add(
        PriceHistory(
            pair_id=pair.id, last_price=current_price, bid_price=current_price, ask_price=current_price,
            observed_at=current_time,
        )
    )
    created = await evaluate_rules_for_pair(
        db_session, pair, current_price, current_price, current_price, current_time, rule_index
    )
    assert created == []


async def test_direction_down_ignores_a_rise(db_session) -> None:
    pair, rule, rule_index = await _make_pair_and_zscore_rule(db_session, direction="down")
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)
    history = _calm_baseline(40)
    await _seed_history(db_session, pair, history, start=base)

    current_time = base + timedelta(seconds=40 * 60)
    current_price = Decimal(str(history[-1] * 1.05))  # sharp RISE; rule only cares about "down"

    db_session.add(
        PriceHistory(
            pair_id=pair.id, last_price=current_price, bid_price=current_price, ask_price=current_price,
            observed_at=current_time,
        )
    )
    created = await evaluate_rules_for_pair(
        db_session, pair, current_price, current_price, current_price, current_time, rule_index
    )
    assert created == []
