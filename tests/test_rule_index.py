"""Pure-logic tests for the bucketing/sorting structure (no DB), plus a real pub/sub
verification using fakeredis (the Redis equivalent of moto — an in-memory server, not
a reimplementation of redis-py's client behavior) since there's no Redis available in
this sandbox to test against for real.
"""

import asyncio
from decimal import Decimal
from uuid import uuid4

import fakeredis.aioredis

from config.settings import settings
from db.models import AlertRule
from workers.rule_index import RuleIndex, keep_index_fresh


def _rule(pair_id, rule_type: str, **kwargs) -> AlertRule:
    kwargs.setdefault("is_enabled", True)
    return AlertRule(pair_id=pair_id, user_id=uuid4(), rule_type=rule_type, **kwargs)


def test_absolute_below_sorted_descending_by_threshold() -> None:
    pair_id = uuid4()
    rules = [
        _rule(pair_id, "absolute_below", threshold=Decimal("50")),
        _rule(pair_id, "absolute_below", threshold=Decimal("100")),
        _rule(pair_id, "absolute_below", threshold=Decimal("75")),
    ]
    index = RuleIndex()
    index.replace_with_rules(rules)

    thresholds = [r.threshold for r in index.rules_for_pair(pair_id).absolute_below]
    assert thresholds == [Decimal("100"), Decimal("75"), Decimal("50")]


def test_absolute_above_sorted_ascending_by_threshold() -> None:
    pair_id = uuid4()
    rules = [
        _rule(pair_id, "absolute_above", threshold=Decimal("50")),
        _rule(pair_id, "absolute_above", threshold=Decimal("100")),
        _rule(pair_id, "absolute_above", threshold=Decimal("75")),
    ]
    index = RuleIndex()
    index.replace_with_rules(rules)

    thresholds = [r.threshold for r in index.rules_for_pair(pair_id).absolute_above]
    assert thresholds == [Decimal("50"), Decimal("75"), Decimal("100")]


def test_zscore_move_unsorted_but_all_present() -> None:
    pair_id = uuid4()
    rules = [
        _rule(pair_id, "zscore_move", sigma=Decimal("3.0")),
        _rule(pair_id, "zscore_move", sigma=Decimal("1.5")),
    ]
    index = RuleIndex()
    index.replace_with_rules(rules)

    sigmas = {r.sigma for r in index.rules_for_pair(pair_id).zscore_move}
    assert sigmas == {Decimal("3.0"), Decimal("1.5")}


def test_pair_with_no_rules_returns_empty_bucket() -> None:
    index = RuleIndex()
    bucket = index.rules_for_pair(uuid4())
    assert bucket.absolute_below == []
    assert bucket.absolute_above == []
    assert bucket.zscore_move == []


def test_buckets_are_keyed_per_pair_not_shared() -> None:
    pair_a, pair_b = uuid4(), uuid4()
    rules = [
        _rule(pair_a, "absolute_below", threshold=Decimal("10")),
        _rule(pair_b, "absolute_below", threshold=Decimal("20")),
    ]
    index = RuleIndex()
    index.replace_with_rules(rules)

    assert [r.threshold for r in index.rules_for_pair(pair_a).absolute_below] == [Decimal("10")]
    assert [r.threshold for r in index.rules_for_pair(pair_b).absolute_below] == [Decimal("20")]


def test_disabled_rules_never_reach_replace_with_rules_are_still_bucketed() -> None:
    # replace_with_rules buckets whatever it's given — filtering by is_enabled is
    # rebuild()'s job (its DB query), not this method's. Documents that boundary.
    pair_id = uuid4()
    rules = [_rule(pair_id, "absolute_below", threshold=Decimal("10"), is_enabled=False)]
    index = RuleIndex()
    index.replace_with_rules(rules)

    assert len(index.rules_for_pair(pair_id).absolute_below) == 1


async def test_invalidation_signal_triggers_rebuild() -> None:
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    index = RuleIndex()

    rebuild_calls = 0
    original_rebuild = index.rebuild

    async def _counting_rebuild() -> int:
        nonlocal rebuild_calls
        rebuild_calls += 1
        return 0

    index.rebuild = _counting_rebuild

    sync_task = asyncio.create_task(keep_index_fresh(index, fake_redis))
    try:
        await asyncio.sleep(0.1)  # let the listener actually subscribe before publishing
        await fake_redis.publish(settings.rule_index_invalidation_channel, "invalidate")
        await asyncio.sleep(0.2)  # let the listener process the message

        assert rebuild_calls == 1
    finally:
        sync_task.cancel()
        index.rebuild = original_rebuild
        try:
            await sync_task
        except asyncio.CancelledError:
            pass


async def test_pubsub_listener_survives_and_ignores_non_message_events() -> None:
    # subscribe() itself generates a "subscribe" confirmation event on the same
    # listen() stream before any real "message" arrives — the listener must not
    # mistake that for an invalidation signal.
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    index = RuleIndex()

    rebuild_calls = 0

    async def _counting_rebuild() -> int:
        nonlocal rebuild_calls
        rebuild_calls += 1
        return 0

    index.rebuild = _counting_rebuild

    sync_task = asyncio.create_task(keep_index_fresh(index, fake_redis))
    try:
        await asyncio.sleep(0.1)
        assert rebuild_calls == 0, "the subscribe confirmation event must not trigger a rebuild"
    finally:
        sync_task.cancel()
        try:
            await sync_task
        except asyncio.CancelledError:
            pass
