"""Evaluates AlertRules against a freshly observed price and fires Notification +
OutboxEvent rows in the same transaction as the PriceHistory write that triggered them.

Only absolute_below/absolute_above are implemented — percent_change, zscore_move, and
spread_widen are Step 7.

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
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import AlertRule, Notification, OutboxEvent, Pair

IMPLEMENTED_RULE_TYPES = ("absolute_below", "absolute_above")
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


def _format_message(pair: Pair, rule: AlertRule, price: Decimal, observed_at: datetime) -> str:
    direction_text = "dropped below" if rule.rule_type == "absolute_below" else "rose above"
    return (
        f"{pair.display_name} {direction_text} {rule.threshold} "
        f"(observed {price} at {observed_at.astimezone(UTC).isoformat()})"
    )


def _in_cooldown(rule: AlertRule, observed_at: datetime) -> bool:
    if rule.last_fired_at is None or rule.cooldown_seconds <= 0:
        return False
    elapsed = (observed_at - rule.last_fired_at).total_seconds()
    return elapsed < rule.cooldown_seconds


async def evaluate_rules_for_pair(
    session: AsyncSession, pair: Pair, price: Decimal, observed_at: datetime
) -> list[Notification]:
    """Fires any absolute_below/absolute_above rule for this pair that the given price
    qualifies for. Returns the Notifications actually created — delivered or
    suppressed — empty if nothing fired or everything was a duplicate of an
    already-recorded detected state.
    """
    result = await session.execute(
        select(AlertRule).where(
            AlertRule.pair_id == pair.id,
            AlertRule.is_enabled.is_(True),
            AlertRule.rule_type.in_(IMPLEMENTED_RULE_TYPES),
        )
    )
    rules = result.scalars().all()

    created: list[Notification] = []

    for rule in rules:
        if not rule_fires(rule, price):
            continue

        state_hash = compute_detected_state_hash(rule, price, observed_at)
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
            detected_price=price,
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
            payload = json.dumps(
                {
                    "notification_id": str(notification.id),
                    "rule_id": str(rule.id),
                    "pair_id": str(pair.id),
                    "pair_display_name": pair.display_name,
                    "rule_type": rule.rule_type,
                    "threshold": str(rule.threshold) if rule.threshold is not None else None,
                    "detected_price": str(price),
                    "triggered_at": observed_at.astimezone(UTC).isoformat(),
                    "message": _format_message(pair, rule, price, observed_at),
                }
            )
            session.add(OutboxEvent(notification_id=notification.id, payload=payload))

        # Advances on every fire, delivered or suppressed, so the cooldown window
        # slides from the most recent qualifying observation.
        rule.last_fired_at = observed_at
        created.append(notification)

    return created
