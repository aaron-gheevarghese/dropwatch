"""Pure-logic tests for rule evaluation and idempotency-key computation — no DB needed.
End-to-end firing + idempotency against a real DB is verified separately (see the
conversation's verification run, not a permanent fixture here — the models depend on
Postgres-specific UUID/Numeric types with no local test DB in this project yet).
"""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from db.models import AlertRule
from rules.evaluator import _matching_absolute_rules, compute_detected_state_hash, compute_idempotency_key, rule_fires


def _make_rule(rule_type: str, threshold: str | None = "100") -> AlertRule:
    return AlertRule(
        id=uuid4(),
        user_id=uuid4(),
        pair_id=uuid4(),
        rule_type=rule_type,
        threshold=Decimal(threshold) if threshold is not None else None,
    )


def test_rule_fires_absolute_below() -> None:
    rule = _make_rule("absolute_below", "100")
    assert rule_fires(rule, Decimal("99")) is True
    assert rule_fires(rule, Decimal("100")) is False
    assert rule_fires(rule, Decimal("101")) is False


def test_rule_fires_absolute_above() -> None:
    rule = _make_rule("absolute_above", "100")
    assert rule_fires(rule, Decimal("101")) is True
    assert rule_fires(rule, Decimal("100")) is False
    assert rule_fires(rule, Decimal("99")) is False


def test_rule_fires_unimplemented_type_returns_false() -> None:
    rule = _make_rule("percent_change", threshold=None)
    assert rule_fires(rule, Decimal("1")) is False


def test_rule_fires_missing_threshold_returns_false() -> None:
    rule = _make_rule("absolute_below", threshold=None)
    assert rule_fires(rule, Decimal("1")) is False


def test_hash_deterministic_for_identical_inputs() -> None:
    rule = _make_rule("absolute_below", "100")
    observed_at = datetime(2026, 8, 14, 12, 30, 15, tzinfo=UTC)

    first = compute_detected_state_hash(rule, Decimal("95"), observed_at)
    second = compute_detected_state_hash(rule, Decimal("95"), observed_at)

    assert first == second


def test_hash_same_within_minute_bucket_despite_different_seconds() -> None:
    # This is the duplicate-poll scenario: same price observed twice within the same
    # 60s cadence window should collapse to one detected state.
    rule = _make_rule("absolute_below", "100")

    first = compute_detected_state_hash(rule, Decimal("95"), datetime(2026, 8, 14, 12, 30, 1, tzinfo=UTC))
    second = compute_detected_state_hash(rule, Decimal("95"), datetime(2026, 8, 14, 12, 30, 47, tzinfo=UTC))

    assert first == second


def test_hash_differs_across_minute_boundary() -> None:
    rule = _make_rule("absolute_below", "100")

    first = compute_detected_state_hash(rule, Decimal("95"), datetime(2026, 8, 14, 12, 30, 59, tzinfo=UTC))
    second = compute_detected_state_hash(rule, Decimal("95"), datetime(2026, 8, 14, 12, 31, 0, tzinfo=UTC))

    assert first != second


def test_hash_differs_for_different_price() -> None:
    rule = _make_rule("absolute_below", "100")
    observed_at = datetime(2026, 8, 14, 12, 30, 0, tzinfo=UTC)

    first = compute_detected_state_hash(rule, Decimal("95"), observed_at)
    second = compute_detected_state_hash(rule, Decimal("94"), observed_at)

    assert first != second


def test_matching_absolute_below_stops_at_first_non_match() -> None:
    # Sorted descending, as workers/rule_index.py guarantees. price=80 should match the
    # 100 and 90 thresholds (80 < both) but not 70 (80 >= 70) — and must stop there,
    # never even looking at 50, proving the short-circuit actually short-circuits.
    pair_id = uuid4()
    rules = [
        AlertRule(id=uuid4(), user_id=uuid4(), pair_id=pair_id, rule_type="absolute_below", threshold=Decimal(t))
        for t in ("100", "90", "70", "50")
    ]
    matched = _matching_absolute_rules(rules, Decimal("80"))
    assert [r.threshold for r in matched] == [Decimal("100"), Decimal("90")]


def test_matching_absolute_above_stops_at_first_non_match() -> None:
    # Sorted ascending. price=120 matches 50 and 100 (120 > both) but not 150.
    pair_id = uuid4()
    rules = [
        AlertRule(id=uuid4(), user_id=uuid4(), pair_id=pair_id, rule_type="absolute_above", threshold=Decimal(t))
        for t in ("50", "100", "150", "200")
    ]
    matched = _matching_absolute_rules(rules, Decimal("120"))
    assert [r.threshold for r in matched] == [Decimal("50"), Decimal("100")]


def test_matching_absolute_rules_none_match() -> None:
    pair_id = uuid4()
    rules = [
        AlertRule(id=uuid4(), user_id=uuid4(), pair_id=pair_id, rule_type="absolute_below", threshold=Decimal(t))
        for t in ("100", "90")
    ]
    assert _matching_absolute_rules(rules, Decimal("200")) == []


def test_matching_absolute_rules_all_match() -> None:
    pair_id = uuid4()
    rules = [
        AlertRule(id=uuid4(), user_id=uuid4(), pair_id=pair_id, rule_type="absolute_below", threshold=Decimal(t))
        for t in ("100", "90", "50")
    ]
    matched = _matching_absolute_rules(rules, Decimal("10"))
    assert len(matched) == 3


def test_idempotency_key_deterministic_and_scoped_to_rule_and_pair() -> None:
    rule_id = uuid4()
    pair_id = uuid4()
    state_hash = "abc123"

    assert compute_idempotency_key(rule_id, pair_id, state_hash) == compute_idempotency_key(
        rule_id, pair_id, state_hash
    )
    assert compute_idempotency_key(rule_id, pair_id, state_hash) != compute_idempotency_key(
        uuid4(), pair_id, state_hash
    )
