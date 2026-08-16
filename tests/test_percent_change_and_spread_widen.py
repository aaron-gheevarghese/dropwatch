"""percent_change and spread_widen: pure-logic fire-check tests, plus DB-integration
tests through the real evaluate_rules_for_pair path (same pattern as
test_zscore_evaluator_integration.py).
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from db.models import AlertRule, Pair, PriceHistory, User
from rules.evaluator import evaluate_rules_for_pair, percent_change_rule_fires, spread_widen_rule_fires
from workers.rule_index import RuleIndex

# ---- pure logic ----


def _rule(rule_type: str, **kwargs) -> AlertRule:
    return AlertRule(id=uuid4(), user_id=uuid4(), pair_id=uuid4(), rule_type=rule_type, **kwargs)


def test_percent_change_up_direction() -> None:
    rule = _rule("percent_change", percent=Decimal("5"), window_seconds=300, direction="up")
    assert percent_change_rule_fires(rule, Decimal("105"), Decimal("100")) is True  # exactly +5%
    assert percent_change_rule_fires(rule, Decimal("104"), Decimal("100")) is False  # +4%, under threshold
    assert percent_change_rule_fires(rule, Decimal("90"), Decimal("100")) is False  # a drop; direction is "up"


def test_percent_change_down_direction() -> None:
    rule = _rule("percent_change", percent=Decimal("5"), window_seconds=300, direction="down")
    assert percent_change_rule_fires(rule, Decimal("95"), Decimal("100")) is True  # exactly -5%
    assert percent_change_rule_fires(rule, Decimal("96"), Decimal("100")) is False
    assert percent_change_rule_fires(rule, Decimal("110"), Decimal("100")) is False  # a rise; direction is "down"


def test_percent_change_both_direction_default() -> None:
    rule = _rule("percent_change", percent=Decimal("5"), window_seconds=300, direction=None)
    assert percent_change_rule_fires(rule, Decimal("105"), Decimal("100")) is True
    assert percent_change_rule_fires(rule, Decimal("95"), Decimal("100")) is True
    assert percent_change_rule_fires(rule, Decimal("102"), Decimal("100")) is False


def test_percent_change_no_past_price_never_fires() -> None:
    rule = _rule("percent_change", percent=Decimal("5"), window_seconds=300)
    assert percent_change_rule_fires(rule, Decimal("1000"), None) is False


def test_percent_change_missing_percent_never_fires() -> None:
    rule = _rule("percent_change", percent=None, window_seconds=300)
    assert percent_change_rule_fires(rule, Decimal("1000"), Decimal("1")) is False


def test_spread_widen_fires_at_or_above_threshold() -> None:
    rule = _rule("spread_widen", percent=Decimal("1"))
    assert spread_widen_rule_fires(rule, bid_price=Decimal("100"), ask_price=Decimal("101")) is True  # exactly 1%
    assert spread_widen_rule_fires(rule, bid_price=Decimal("100"), ask_price=Decimal("100.5")) is False  # 0.5%
    assert spread_widen_rule_fires(rule, bid_price=Decimal("100"), ask_price=Decimal("102")) is True  # 2%


def test_spread_widen_missing_percent_never_fires() -> None:
    rule = _rule("spread_widen", percent=None)
    assert spread_widen_rule_fires(rule, bid_price=Decimal("100"), ask_price=Decimal("110")) is False


# ---- DB integration, mirrors workers/poller.py's real call pattern ----


async def _make_pair_and_user(session) -> tuple[Pair, User]:
    user = User(contact="test@example.com")
    pair = Pair(
        kraken_pair_name=f"TESTPCT{uuid4().hex[:8].upper()}USD",
        display_name="TEST/USD",
        base_currency="TEST",
        quote_currency="USD",
        poll_interval_seconds=60,
        is_active=True,
    )
    session.add_all([user, pair])
    await session.flush()
    return pair, user


async def test_percent_change_fires_against_real_history(db_session) -> None:
    pair, user = await _make_pair_and_user(db_session)
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)

    # Price was 100 five minutes ago, flat since.
    db_session.add(
        PriceHistory(
            pair_id=pair.id, last_price=Decimal("100"), bid_price=Decimal("100"), ask_price=Decimal("100"),
            observed_at=base,
        )
    )
    await db_session.flush()

    rule = AlertRule(
        user_id=user.id, pair_id=pair.id, rule_type="percent_change",
        percent=Decimal("5"), window_seconds=300, direction="up", is_enabled=True,
    )
    db_session.add(rule)
    await db_session.flush()
    rule_index = RuleIndex()
    rule_index.replace_with_rules([rule])

    current_time = base + timedelta(seconds=300)
    current_price = Decimal("106")  # +6% over the 300s window
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
    assert created[0].type == "percent_change"


async def test_percent_change_insufficient_history_never_fires(db_session) -> None:
    pair, user = await _make_pair_and_user(db_session)
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)

    rule = AlertRule(
        user_id=user.id, pair_id=pair.id, rule_type="percent_change",
        percent=Decimal("1"), window_seconds=300, is_enabled=True,
    )
    db_session.add(rule)
    await db_session.flush()
    rule_index = RuleIndex()
    rule_index.replace_with_rules([rule])

    # No history at all older than the window -- nothing to compare against.
    current_price = Decimal("1000000")
    db_session.add(
        PriceHistory(
            pair_id=pair.id, last_price=current_price, bid_price=current_price, ask_price=current_price,
            observed_at=base,
        )
    )
    created = await evaluate_rules_for_pair(
        db_session, pair, current_price, current_price, current_price, base, rule_index
    )
    assert created == []


async def test_percent_change_shares_lookup_across_rules_with_same_window(db_session) -> None:
    pair, user = await _make_pair_and_user(db_session)
    base = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)

    db_session.add(
        PriceHistory(
            pair_id=pair.id, last_price=Decimal("100"), bid_price=Decimal("100"), ask_price=Decimal("100"),
            observed_at=base,
        )
    )
    await db_session.flush()

    # Two rules, same window_seconds, different percent thresholds -- both should see
    # the same +6% move but only the looser one should fire.
    tight_rule = AlertRule(
        user_id=user.id, pair_id=pair.id, rule_type="percent_change",
        percent=Decimal("10"), window_seconds=300, direction="up", is_enabled=True,
    )
    loose_rule = AlertRule(
        user_id=user.id, pair_id=pair.id, rule_type="percent_change",
        percent=Decimal("5"), window_seconds=300, direction="up", is_enabled=True,
    )
    db_session.add_all([tight_rule, loose_rule])
    await db_session.flush()
    rule_index = RuleIndex()
    rule_index.replace_with_rules([tight_rule, loose_rule])

    current_time = base + timedelta(seconds=300)
    current_price = Decimal("106")
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
    assert created[0].rule_id == loose_rule.id


async def test_spread_widen_fires_against_real_bid_ask(db_session) -> None:
    pair, user = await _make_pair_and_user(db_session)

    rule = AlertRule(
        user_id=user.id, pair_id=pair.id, rule_type="spread_widen", percent=Decimal("1"), is_enabled=True
    )
    db_session.add(rule)
    await db_session.flush()
    rule_index = RuleIndex()
    rule_index.replace_with_rules([rule])

    observed_at = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)
    last_price = Decimal("100.5")
    bid_price = Decimal("100")
    ask_price = Decimal("102")  # 2% spread, over the 1% threshold

    created = await evaluate_rules_for_pair(db_session, pair, last_price, bid_price, ask_price, observed_at, rule_index)
    assert len(created) == 1
    assert created[0].type == "spread_widen"


async def test_spread_widen_does_not_fire_on_a_tight_spread(db_session) -> None:
    pair, user = await _make_pair_and_user(db_session)

    rule = AlertRule(
        user_id=user.id, pair_id=pair.id, rule_type="spread_widen", percent=Decimal("1"), is_enabled=True
    )
    db_session.add(rule)
    await db_session.flush()
    rule_index = RuleIndex()
    rule_index.replace_with_rules([rule])

    observed_at = datetime(2026, 8, 15, 8, 0, 0, tzinfo=UTC)
    last_price = Decimal("100.05")
    bid_price = Decimal("100")
    ask_price = Decimal("100.1")  # 0.1% spread, under the 1% threshold

    created = await evaluate_rules_for_pair(db_session, pair, last_price, bid_price, ask_price, observed_at, rule_index)
    assert created == []
