"""Evaluates AlertRules against a freshly observed price and fires Notification +
OutboxEvent rows in the same transaction as the PriceHistory write that triggered them.

All five rule types are implemented: absolute_below, absolute_above, zscore_move,
percent_change, spread_widen.

percent_change compares the current price against the price "as of" window_seconds ago
— defined as the most recent real observation at or before that instant (never a
row chronologically after it — no look-ahead — and never interpolated: if the pair
has less than window_seconds of history, there's nothing to compare against and the
rule can't fire, same "insufficient history means no fire" treatment as zscore_move).
Different percent_change rules for the same pair can have different window_seconds, so
unlike zscore_move's one-shared-value-per-pair pattern, the past-price lookup is cached
per distinct window_seconds within one evaluation, not computed once for the whole pair.

spread_widen needs bid/ask, not just last_price, so evaluate_rules_for_pair takes both
alongside last_price now.

`percent` fields are percentage numbers, not raw fractions (0.5 means 0.5%, matching
zscore_zero_variance_min_percent's existing convention) — a rule's underlying formula is
always computed as a raw ratio and then multiplied by 100 before comparing against
`percent`.

Rules come from the in-memory RuleIndex (workers/rule_index.py), not a per-pair DB
query — that's Step 6's whole point. Rule objects in the index were loaded by a
different, now-closed session, so they're SQLAlchemy-detached: reading their columns
(threshold, sigma, rule_type, id, ...) is fine, they're already loaded in memory, but
mutating an attribute and expecting a later session.commit() to notice is not — a
detached object's changes aren't tracked by any session. last_fired_at is therefore
updated two ways: mutated directly on the in-memory object (so the index's own cooldown
check stays correct for later evaluations in this same process, without waiting for a
rebuild) AND written to Postgres via a targeted UPDATE by id (durable, visible to other
processes and a future GET /rules). Note the index can lag a brand-new rule by however
long invalidation takes to propagate — an accepted eventual-consistency window, not a
bug; see workers/rule_index.py.

Two independent guards apply, in order:

1. Idempotency: the SAME detected state (same rule, same pair, same price, same minute)
   collapses to exactly one Notification via the unique idempotency_key, even under
   concurrent evaluation (e.g. two overlapping poll cycles) — a losing concurrent
   INSERT is caught via a SAVEPOINT and treated as "already handled", not an error.
2. Cooldown: a genuinely new detected state that fires within cooldown_seconds of the
   rule's last_fired_at is still recorded — as a Notification with
   status="suppressed_cooldown" — but does not get an OutboxEvent, so it's never
   delivered. last_fired_at advances on every fire, delivered or suppressed, so the
   cooldown window slides forward from the most recent qualifying observation rather
   than only from delivered ones.
"""

import hashlib
import json
import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from db.models import AlertRule, Notification, OutboxEvent, Pair, PriceHistory
from workers.rule_index import RuleIndex
from workers.zscore_filter import ZScoreResult, compute_zscore

STATUS_PENDING = "pending"
STATUS_SUPPRESSED_COOLDOWN = "suppressed_cooldown"


def compute_detected_state_hash(rule: AlertRule, price: Decimal, observed_at: datetime) -> str:
    # Minute bucket, matching the system's own poll cadence: two polls landing in the
    # same 60s window are the same "detected state" for dedup purposes, even if their
    # exact observed_at differs by a few seconds (e.g. a duplicate/redelivered poll).
    bucket = observed_at.astimezone(UTC).replace(second=0, microsecond=0)

    parts = [
        rule.rule_type,
        str(rule.threshold) if rule.threshold is not None else "",
        str(rule.percent) if rule.percent is not None else "",
        str(rule.window_seconds) if rule.window_seconds is not None else "",
        str(rule.sigma) if rule.sigma is not None else "",
        rule.direction or "",
        str(price),
        bucket.isoformat(),
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def compute_idempotency_key(rule_id: UUID, pair_id: UUID, detected_state_hash: str) -> str:
    raw = f"{rule_id}:{pair_id}:{detected_state_hash}"
    return hashlib.sha256(raw.encode()).hexdigest()


def rule_fires(rule: AlertRule, price: Decimal) -> bool:
    if rule.rule_type == "absolute_below":
        return rule.threshold is not None and price < rule.threshold
    if rule.rule_type == "absolute_above":
        return rule.threshold is not None and price > rule.threshold
    return False


def zscore_rule_fires(rule: AlertRule, result: ZScoreResult | None, zero_variance_min_percent: Decimal) -> bool:
    if result is None or rule.sigma is None:
        return False

    direction = rule.direction or "both"

    if result.is_zero_variance:
        # e^r - 1 is the exact percent change (log-return only approximates percent
        # change for small moves, and a flat/zero-variance window is precisely the
        # degenerate case where that approximation shouldn't be trusted).
        percent_change = abs(math.expm1(result.current_return)) * 100
        if percent_change < float(zero_variance_min_percent):
            return False
        if direction == "down":
            return result.current_return < 0
        if direction == "up":
            return result.current_return > 0
        return True

    sigma = float(rule.sigma)
    z = result.z
    if direction == "down":
        return z <= -sigma
    if direction == "up":
        return z >= sigma
    return abs(z) >= sigma


def percent_change_rule_fires(rule: AlertRule, current_price: Decimal, past_price: Decimal | None) -> bool:
    if past_price is None or past_price == 0 or rule.percent is None:
        return False

    change_percent = (current_price - past_price) / past_price * 100
    direction = rule.direction or "both"
    if direction == "down":
        return change_percent <= -rule.percent
    if direction == "up":
        return change_percent >= rule.percent
    return abs(change_percent) >= rule.percent


def spread_widen_rule_fires(rule: AlertRule, bid_price: Decimal, ask_price: Decimal) -> bool:
    if rule.percent is None or bid_price is None or bid_price == 0:
        return False
    spread_percent = (ask_price - bid_price) / bid_price * 100
    return spread_percent >= rule.percent


async def _fetch_price_at_or_before(session: AsyncSession, pair: Pair, cutoff: datetime) -> Decimal | None:
    # "Price as of window_seconds ago" = the most recent real observation at or before
    # that instant — never a row chronologically after it (no look-ahead), never
    # interpolated (if none exists, there's genuinely nothing to compare against).
    result = await session.execute(
        select(PriceHistory.last_price)
        .where(PriceHistory.pair_id == pair.id, PriceHistory.observed_at <= cutoff)
        .order_by(PriceHistory.observed_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _fetch_recent_prices(session: AsyncSession, pair: Pair, limit: int) -> list[Decimal]:
    # Ordinary SELECT — autoflush means the PriceHistory row the poller just added for
    # this exact observation (pending, not yet committed) is flushed and visible here
    # within the same transaction, so it's naturally included as the newest price with
    # no special-casing. No interpolation happens anywhere in this query: it returns
    # whatever real rows exist, however irregularly spaced a gap left them.
    result = await session.execute(
        select(PriceHistory.last_price)
        .where(PriceHistory.pair_id == pair.id)
        .order_by(PriceHistory.observed_at.desc())
        .limit(limit)
    )
    prices_desc = list(result.scalars())
    return list(reversed(prices_desc))


def _format_message(
    pair: Pair,
    rule: AlertRule,
    last_price: Decimal,
    observed_at: datetime,
    zscore_result: ZScoreResult | None = None,
    past_price: Decimal | None = None,
    bid_price: Decimal | None = None,
    ask_price: Decimal | None = None,
) -> str:
    timestamp = observed_at.astimezone(UTC).isoformat()

    if rule.rule_type == "zscore_move":
        assert zscore_result is not None
        percent = math.expm1(zscore_result.current_return) * 100
        if zscore_result.is_zero_variance:
            detail = f"flat-window fallback move of {percent:+.3f}% (baseline window had zero variance)"
        else:
            detail = f"z={zscore_result.z:+.2f} (sigma threshold {rule.sigma}, return {percent:+.3f}%)"
        return f"{pair.display_name} z-score move: {detail} (observed {last_price} at {timestamp})"

    if rule.rule_type == "percent_change":
        assert past_price is not None
        change_percent = (last_price - past_price) / past_price * 100
        return (
            f"{pair.display_name} moved {change_percent:+.3f}% over {rule.window_seconds}s "
            f"(threshold {rule.percent}%, direction {rule.direction or 'both'}; "
            f"{past_price} -> {last_price} at {timestamp})"
        )

    if rule.rule_type == "spread_widen":
        assert bid_price is not None and ask_price is not None
        spread_percent = (ask_price - bid_price) / bid_price * 100
        return (
            f"{pair.display_name} spread widened to {spread_percent:.3f}% "
            f"(threshold {rule.percent}%; bid={bid_price} ask={ask_price} at {timestamp})"
        )

    direction_text = "dropped below" if rule.rule_type == "absolute_below" else "rose above"
    return f"{pair.display_name} {direction_text} {rule.threshold} (observed {last_price} at {timestamp})"


def _in_cooldown(rule: AlertRule, observed_at: datetime) -> bool:
    if rule.last_fired_at is None or rule.cooldown_seconds <= 0:
        return False
    elapsed = (observed_at - rule.last_fired_at).total_seconds()
    return elapsed < rule.cooldown_seconds


def _matching_absolute_rules(rules: list[AlertRule], price: Decimal) -> list[AlertRule]:
    # Sorted so that the moment one rule fails to fire, every remaining rule in this
    # bucket is guaranteed to fail too (see workers/rule_index.py) — stop right there
    # instead of checking the rest.
    matched = []
    for rule in rules:
        if not rule_fires(rule, price):
            break
        matched.append(rule)
    return matched


async def evaluate_rules_for_pair(
    session: AsyncSession,
    pair: Pair,
    last_price: Decimal,
    bid_price: Decimal,
    ask_price: Decimal,
    observed_at: datetime,
    rule_index: RuleIndex,
) -> list[Notification]:
    """Fires any rule for this pair that the given observation qualifies for, across all
    five implemented rule types. Returns the Notifications actually created — delivered
    or suppressed — empty if nothing fired or everything was a duplicate of an
    already-recorded detected state.
    """
    pair_rules = rule_index.rules_for_pair(pair.id)

    candidates: list[AlertRule] = _matching_absolute_rules(pair_rules.absolute_below, last_price)
    candidates += _matching_absolute_rules(pair_rules.absolute_above, last_price)

    zscore_result: ZScoreResult | None = None
    if pair_rules.zscore_move:
        prices = await _fetch_recent_prices(session, pair, settings.zscore_window + 2)
        zscore_result = compute_zscore(
            prices, window=settings.zscore_window, min_observations=settings.zscore_min_observations
        )
        candidates += [
            rule
            for rule in pair_rules.zscore_move
            if zscore_rule_fires(rule, zscore_result, settings.zscore_zero_variance_min_percent)
        ]

    # Different percent_change rules can use different window_seconds, so there's no
    # single shared baseline the way zscore has — but rules sharing the same
    # window_seconds share one lookup rather than each re-querying.
    past_price_by_rule_id: dict[UUID, Decimal | None] = {}
    if pair_rules.percent_change:
        past_price_by_window: dict[int, Decimal | None] = {}
        for rule in pair_rules.percent_change:
            window = rule.window_seconds
            if window not in past_price_by_window:
                cutoff = observed_at - timedelta(seconds=window)
                past_price_by_window[window] = await _fetch_price_at_or_before(session, pair, cutoff)
            past_price = past_price_by_window[window]
            past_price_by_rule_id[rule.id] = past_price
            if percent_change_rule_fires(rule, last_price, past_price):
                candidates.append(rule)

    if pair_rules.spread_widen:
        candidates += [
            rule for rule in pair_rules.spread_widen if spread_widen_rule_fires(rule, bid_price, ask_price)
        ]

    created: list[Notification] = []

    for rule in candidates:
        state_hash = compute_detected_state_hash(rule, last_price, observed_at)
        idempotency_key = compute_idempotency_key(rule.id, pair.id, state_hash)

        existing = await session.scalar(
            select(Notification.id).where(Notification.idempotency_key == idempotency_key)
        )
        if existing is not None:
            continue

        suppressed = _in_cooldown(rule, observed_at)

        notification = Notification(
            rule_id=rule.id,
            pair_id=pair.id,
            type=rule.rule_type,
            detected_price=last_price,
            detected_state_hash=state_hash,
            idempotency_key=idempotency_key,
            status=STATUS_SUPPRESSED_COOLDOWN if suppressed else STATUS_PENDING,
        )

        # A SAVEPOINT, not the outer transaction: if a concurrent evaluation already
        # committed this exact idempotency_key between our SELECT above and this INSERT,
        # only this notification's insert rolls back — the PriceHistory write and any
        # other pairs/rules in the same poller batch are untouched.
        try:
            async with session.begin_nested():
                session.add(notification)
                await session.flush()  # assigns notification.id for the OutboxEvent FK below
        except IntegrityError:
            continue

        if not suppressed:
            rule_past_price = past_price_by_rule_id.get(rule.id)
            payload = json.dumps(
                {
                    "notification_id": str(notification.id),
                    "rule_id": str(rule.id),
                    "pair_id": str(pair.id),
                    "pair_display_name": pair.display_name,
                    "rule_type": rule.rule_type,
                    "threshold": str(rule.threshold) if rule.threshold is not None else None,
                    "percent": str(rule.percent) if rule.percent is not None else None,
                    "window_seconds": rule.window_seconds,
                    "sigma": str(rule.sigma) if rule.sigma is not None else None,
                    "direction": rule.direction,
                    "z_score": zscore_result.z if (rule.rule_type == "zscore_move" and zscore_result) else None,
                    "past_price": str(rule_past_price) if rule_past_price is not None else None,
                    "bid_price": str(bid_price) if rule.rule_type == "spread_widen" else None,
                    "ask_price": str(ask_price) if rule.rule_type == "spread_widen" else None,
                    "detected_price": str(last_price),
                    "triggered_at": observed_at.astimezone(UTC).isoformat(),
                    "message": _format_message(
                        pair, rule, last_price, observed_at, zscore_result, rule_past_price, bid_price, ask_price
                    ),
                }
            )
            session.add(OutboxEvent(notification_id=notification.id, payload=payload))

        # Advances on every fire, delivered or suppressed, so the cooldown window
        # slides from the most recent qualifying observation. rule is detached (loaded
        # by the index's own session, not this one), so a plain attribute mutation
        # alone would never be flushed — mutate the in-memory copy (keeps this
        # process's cooldown checks correct immediately) AND write it through
        # explicitly by id (durable, visible to other processes and the DB directly).
        rule.last_fired_at = observed_at
        await session.execute(update(AlertRule).where(AlertRule.id == rule.id).values(last_fired_at=observed_at))
        created.append(notification)

    return created
