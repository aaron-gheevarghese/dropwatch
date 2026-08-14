"""In-memory index of enabled AlertRules, bucketed by pair_id, so the poller's hot path
(evaluate rules for a pair on every poll) does zero DB round-trips per pair instead of
one SELECT per pair per poll cycle.

Within each pair's bucket:
  - absolute_below sorted DESCENDING by threshold (largest/easiest-to-fire first).
    Once a threshold fails (price >= threshold), every smaller threshold also fails —
    price >= threshold_big > threshold_small implies price > threshold_small too — so
    evaluation can stop at the first non-match instead of checking every rule.
  - absolute_above sorted ASCENDING by threshold — same logic, mirrored.
  - zscore_move has no such monotonic ordering per rule (firing depends on sigma AND
    direction against a single shared z-score computed once per pair), and the
    per-rule check is O(1) once that z-score is known, so there's nothing to
    short-circuit — it's just a small unsorted list.

Freshness: rebuilt from Postgres on worker start, on a Redis pub/sub invalidation
signal (published by POST/PATCH /rules), and periodically as a safety net in case a
worker misses a signal (e.g. briefly disconnected from Redis) — the periodic refresh is
a fallback, not a substitute for the pub/sub path. A rebuild is atomic from a reader's
perspective: the new bucket dict is fully built off to the side, then swapped in with a
single reference assignment, so concurrent evaluation never sees a partially-rebuilt
index.
"""

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import select

from config.settings import settings
from db.client import async_session_factory
from db.models import AlertRule
from rules.rule_types import IMPLEMENTED_RULE_TYPES

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PairRules:
    absolute_below: list[AlertRule] = field(default_factory=list)  # sorted desc by threshold
    absolute_above: list[AlertRule] = field(default_factory=list)  # sorted asc by threshold
    zscore_move: list[AlertRule] = field(default_factory=list)  # unsorted


_EMPTY_PAIR_RULES = PairRules()


def _bucket_rules(rules: list[AlertRule]) -> dict[UUID, PairRules]:
    grouped: dict[UUID, dict[str, list[AlertRule]]] = defaultdict(
        lambda: {rule_type: [] for rule_type in IMPLEMENTED_RULE_TYPES}
    )
    for rule in rules:
        grouped[rule.pair_id][rule.rule_type].append(rule)

    return {
        pair_id: PairRules(
            absolute_below=sorted(by_type["absolute_below"], key=lambda r: r.threshold, reverse=True),
            absolute_above=sorted(by_type["absolute_above"], key=lambda r: r.threshold),
            zscore_move=by_type["zscore_move"],
        )
        for pair_id, by_type in grouped.items()
    }


async def _load_buckets_from_db() -> dict[UUID, PairRules]:
    async with async_session_factory() as session:
        result = await session.execute(
            select(AlertRule).where(
                AlertRule.is_enabled.is_(True),
                AlertRule.rule_type.in_(IMPLEMENTED_RULE_TYPES),
            )
        )
        rules = result.scalars().all()
    return _bucket_rules(rules)


class RuleIndex:
    def __init__(self) -> None:
        self._buckets: dict[UUID, PairRules] = {}

    def rules_for_pair(self, pair_id: UUID) -> PairRules:
        return self._buckets.get(pair_id, _EMPTY_PAIR_RULES)

    async def rebuild(self) -> int:
        buckets = await _load_buckets_from_db()
        self._buckets = buckets  # atomic reference swap
        return sum(len(b.absolute_below) + len(b.absolute_above) + len(b.zscore_move) for b in buckets.values())

    def replace_with_rules(self, rules: list[AlertRule]) -> None:
        """Builds buckets directly from the given rules, bypassing Postgres entirely.
        Real production code always goes through rebuild() — this exists for tests that
        need an index reflecting rules still inside an uncommitted transaction, which
        rebuild()'s own separate connection can't see.
        """
        self._buckets = _bucket_rules(rules)


# Bounded read, deliberately not pubsub.listen()'s unbounded block(True) read.
# listen() calls parse_response(block=True) in a loop — with no timeout, a single read
# blocks forever, and redis-py's health-check PING (check_health()) only runs at the
# START of a parse_response call. If the connection dies silently (e.g. NAT/Docker
# networking drops an idle connection without a FIN/RST reaching the client — common
# in cloud environments, and exactly what happened in production), listen() just hangs
# on that one dead read forever: no exception, no log line, nothing. Polling with
# get_message(timeout=...) instead means every read returns on its own on a schedule,
# giving check_health() a chance to fire (see cache/client.py's health_check_interval)
# and actually detect the dead connection — at which point it raises, our except below
# catches it, and the reconnect loop kicks in for real. This class of bug can't be
# reproduced against fakeredis: there's no real TCP layer for a connection to silently
# die on, which is exactly why it passed there and hung in production.
GET_MESSAGE_TIMEOUT_SECONDS = 10
RECONNECT_BACKOFF_SECONDS = 10


async def _listen_for_invalidations(index: RuleIndex, redis_client: Redis) -> None:
    channel = settings.rule_index_invalidation_channel
    while True:
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(channel)
            logger.info("rule index: subscribed to channel %r, listening for invalidations", channel)
            try:
                while True:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=GET_MESSAGE_TIMEOUT_SECONDS
                    )
                    if message is None:
                        logger.debug("rule index: poll on %r returned nothing (idle)", channel)
                        continue

                    logger.info("rule index: invalidation received on %r: %r", channel, message)
                    try:
                        count = await index.rebuild()
                        logger.info("rule index: rebuilt with %d rules after invalidation", count)
                    except Exception:
                        logger.exception("rule index: rebuild after invalidation failed; keeping previous index")
            finally:
                await pubsub.unsubscribe(channel)
        except Exception:
            # Redis unreachable, connection dropped/detected-dead by the health check,
            # etc. Don't crash the worker over this — the periodic refresh below covers
            # the gap until reconnection succeeds.
            logger.exception("rule index: pub/sub listener error, reconnecting in %ds", RECONNECT_BACKOFF_SECONDS)
            await asyncio.sleep(RECONNECT_BACKOFF_SECONDS)


async def _periodic_refresh(index: RuleIndex) -> None:
    while True:
        await asyncio.sleep(settings.rule_index_refresh_interval_seconds)
        try:
            count = await index.rebuild()
            logger.info("rule index: periodic refresh rebuilt with %d rules", count)
        except Exception:
            logger.exception("rule index: periodic refresh failed")


async def keep_index_fresh(index: RuleIndex, redis_client: Redis) -> None:
    """Runs forever, keeping an already-built index fresh via pub/sub + periodic
    refresh. Deliberately does NOT do the initial build itself — call index.rebuild()
    and await it before accepting any work, then spawn this as a background task.
    Doing the initial build inside this function would let a caller start processing
    before it completes (create_task doesn't wait), silently evaluating against an
    empty index for however long that takes.
    """
    await asyncio.gather(_listen_for_invalidations(index, redis_client), _periodic_refresh(index))
