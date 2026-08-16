import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from db.client import async_session_factory, get_session
from db.models import AlertRule, Notification, Pair, PriceHistory, User
from rules.evaluator import STATUS_PENDING, evaluate_rules_for_pair
from rules.rule_types import IMPLEMENTED_RULE_TYPES
from workers.rule_index import RuleIndex

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/rules", tags=["rules"])


async def get_redis(request: Request):
    return request.app.state.redis


async def _publish_invalidation(redis_client, reason: str) -> None:
    # Best-effort: the rule write already committed to Postgres (source of truth) by
    # the time this runs. If Redis is unreachable, workers still pick this up via their
    # periodic safety-net refresh (workers/rule_index.py) — just not instantly. Don't
    # fail the request over a cache-invalidation hiccup.
    channel = settings.rule_index_invalidation_channel
    logger.info("rule index: publishing invalidation on %r (%s)", channel, reason)
    try:
        subscriber_count = await redis_client.publish(channel, "invalidate")
    except Exception:
        logger.exception(
            "rule index: failed to publish invalidation on %r (%s); "
            "workers will pick this up on their next periodic refresh",
            channel,
            reason,
        )
        return

    # publish() returning 0 means Redis accepted the command but NO worker was
    # subscribed at that instant — a strong signal something's wrong with the
    # subscriber side (not connected, crashed, wrong channel/URL) even though this
    # publish call itself "succeeded". Not an error here (nothing to retry — Redis
    # pub/sub doesn't queue for absent subscribers), but very much worth knowing.
    if subscriber_count == 0:
        logger.warning(
            "rule index: published invalidation on %r (%s) but 0 subscribers received it — "
            "no worker appears to be listening right now",
            channel,
            reason,
        )
    else:
        logger.info(
            "rule index: invalidation on %r (%s) delivered to %d subscriber(s)",
            channel,
            reason,
            subscriber_count,
        )

# Shared by zscore_move and percent_change — both have a directional sense (a move can
# be "up", "down", or either); spread_widen and the absolute_* types don't use this.
DIRECTIONS = ("up", "down", "both")


def _validate_rule_fields(
    rule_type: str,
    threshold: Decimal | None,
    percent: Decimal | None,
    window_seconds: int | None,
    sigma: Decimal | None,
    direction: str | None,
) -> None:
    if rule_type not in IMPLEMENTED_RULE_TYPES:
        raise ValueError(f"rule_type must be one of {IMPLEMENTED_RULE_TYPES}")
    if rule_type in ("absolute_below", "absolute_above") and threshold is None:
        raise ValueError(f"threshold is required for rule_type={rule_type!r}")
    if rule_type == "zscore_move":
        if sigma is None:
            raise ValueError(f"sigma is required for rule_type={rule_type!r}")
        if direction is not None and direction not in DIRECTIONS:
            raise ValueError(f"direction must be one of {DIRECTIONS} for rule_type={rule_type!r}")
    if rule_type == "percent_change":
        if percent is None:
            raise ValueError(f"percent is required for rule_type={rule_type!r}")
        if window_seconds is None:
            raise ValueError(f"window_seconds is required for rule_type={rule_type!r}")
        if direction is not None and direction not in DIRECTIONS:
            raise ValueError(f"direction must be one of {DIRECTIONS} for rule_type={rule_type!r}")
    if rule_type == "spread_widen" and percent is None:
        raise ValueError(f"percent is required for rule_type={rule_type!r}")


class CreateRuleRequest(BaseModel):
    user_id: UUID
    pair_id: UUID
    rule_type: str
    threshold: Decimal | None = None
    percent: Decimal | None = None
    window_seconds: int | None = None
    sigma: Decimal | None = None
    direction: str | None = None
    cooldown_seconds: int = Field(default=0, ge=0)
    is_enabled: bool = True

    @model_validator(mode="after")
    def _validate(self) -> "CreateRuleRequest":
        _validate_rule_fields(
            self.rule_type, self.threshold, self.percent, self.window_seconds, self.sigma, self.direction
        )
        return self


class UpdateRuleRequest(BaseModel):
    """Partial update — only fields explicitly provided are changed. user_id, pair_id,
    and rule_type are intentionally not patchable: changing what a rule fundamentally
    is or belongs to is closer to delete-and-recreate than a modification.
    """

    threshold: Decimal | None = None
    percent: Decimal | None = None
    window_seconds: int | None = None
    sigma: Decimal | None = None
    direction: str | None = None
    cooldown_seconds: int | None = Field(default=None, ge=0)
    is_enabled: bool | None = None

    model_config = {"extra": "forbid"}


class RuleResponse(BaseModel):
    id: UUID
    user_id: UUID
    pair_id: UUID
    rule_type: str
    threshold: Decimal | None
    percent: Decimal | None
    window_seconds: int | None
    sigma: Decimal | None
    direction: str | None
    cooldown_seconds: int
    is_enabled: bool
    last_fired_at: datetime | None
    created_at: datetime

    model_config = {"from_attributes": True}


@router.post("", response_model=RuleResponse, status_code=status.HTTP_201_CREATED)
async def create_rule(
    body: CreateRuleRequest,
    session: AsyncSession = Depends(get_session),
    redis_client=Depends(get_redis),
) -> AlertRule:
    pair = await session.get(Pair, body.pair_id)
    if pair is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"pair {body.pair_id} not found")

    user = await session.get(User, body.user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"user {body.user_id} not found")

    rule = AlertRule(
        user_id=body.user_id,
        pair_id=body.pair_id,
        rule_type=body.rule_type,
        threshold=body.threshold,
        percent=body.percent,
        window_seconds=body.window_seconds,
        sigma=body.sigma,
        direction=body.direction,
        cooldown_seconds=body.cooldown_seconds,
        is_enabled=body.is_enabled,
    )
    session.add(rule)
    await session.commit()
    await session.refresh(rule)
    await _publish_invalidation(redis_client, reason=f"rule {rule.id} created")
    return rule


@router.patch("/{rule_id}", response_model=RuleResponse)
async def update_rule(
    rule_id: UUID,
    body: UpdateRuleRequest,
    session: AsyncSession = Depends(get_session),
    redis_client=Depends(get_redis),
) -> AlertRule:
    rule = await session.get(AlertRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"rule {rule_id} not found")

    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(rule, field, value)

    try:
        _validate_rule_fields(
            rule.rule_type, rule.threshold, rule.percent, rule.window_seconds, rule.sigma, rule.direction
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    await session.commit()
    await session.refresh(rule)
    await _publish_invalidation(redis_client, reason=f"rule {rule.id} updated")
    return rule


class BacktestRequest(BaseModel):
    """Same rule-definition shape as CreateRuleRequest, minus user_id (there's no real
    owner — nothing is being attributed or saved) and is_enabled (a backtest evaluates
    the given definition unconditionally; enabled/disabled is a toggle for saved rules,
    not meaningful for a hypothetical one). Plus lookback_days, which POST /rules has no
    equivalent of.
    """

    pair_id: UUID
    rule_type: str
    threshold: Decimal | None = None
    percent: Decimal | None = None
    window_seconds: int | None = None
    sigma: Decimal | None = None
    direction: str | None = None
    cooldown_seconds: int = Field(default=0, ge=0)
    lookback_days: int = Field(gt=0, le=365)

    @model_validator(mode="after")
    def _validate(self) -> "BacktestRequest":
        _validate_rule_fields(
            self.rule_type, self.threshold, self.percent, self.window_seconds, self.sigma, self.direction
        )
        return self


class BacktestFireEvent(BaseModel):
    triggered_at: datetime
    detected_price: Decimal
    status: str  # "pending" (would have delivered) or "suppressed_cooldown"


class BacktestResponse(BaseModel):
    pair_id: UUID
    rule_type: str
    lookback_days: int
    observations_replayed: int
    data_start: datetime | None
    data_end: datetime | None
    fire_count: int
    fires_per_day: float
    post_cooldown_fire_count: int
    fires: list[BacktestFireEvent]


@router.post("/backtest", response_model=BacktestResponse)
async def backtest_rule(body: BacktestRequest) -> BacktestResponse:
    """Replays an unsaved rule definition against real PriceHistory for one pair.

    Reuses evaluate_rules_for_pair — the exact same function the live poller calls —
    row by row over history, instead of a parallel/duplicate evaluation implementation.
    That's what keeps backtest results honest: whatever this returns is what the live
    engine would actually have done, not an approximation of it.

    Persists nothing: runs in its own session that's never committed. The temp rule
    itself, and every Notification/OutboxEvent evaluate_rules_for_pair creates during
    replay, are genuinely session.add()/flush()'d — real inserts within the
    transaction, including satisfying real foreign keys (rule_id -> alert_rules.id) —
    but flush is not commit. Nothing survives once this session closes without one, the
    same rollback-on-close behavior verified in tests/test_fixture_isolation.py.
    """
    async with async_session_factory() as session:
        pair = await session.get(Pair, body.pair_id)
        if pair is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"pair {body.pair_id} not found")

        # notifications.rule_id (and alert_rules.user_id) are real foreign keys —
        # Postgres enforces them even mid-transaction, before anything commits. A rule
        # that's never inserted at all fails that check on every single fire, which
        # was silently swallowed by the same IntegrityError handler built for the
        # concurrent-race case in evaluate_rules_for_pair, producing a false
        # fire_count=0 for every backtest regardless of the rule. So: add and flush the
        # temp rule for real, satisfying the FK within this transaction — just never
        # commit. That's what actually makes "no persistence" true here, not skipping
        # the insert. user_id borrows the real (v1: single) seeded user, since
        # fabricating one would hit the exact same FK problem.
        user = await session.scalar(select(User).limit(1))
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="no user seeded — run scripts/seed_user before using the backtest endpoint",
            )

        temp_rule = AlertRule(
            id=uuid4(),
            user_id=user.id,
            pair_id=body.pair_id,
            rule_type=body.rule_type,
            threshold=body.threshold,
            percent=body.percent,
            window_seconds=body.window_seconds,
            sigma=body.sigma,
            direction=body.direction,
            cooldown_seconds=body.cooldown_seconds,
            is_enabled=True,
        )
        session.add(temp_rule)
        await session.flush()

        rule_index = RuleIndex()
        rule_index.replace_with_rules([temp_rule])

        cutoff = datetime.now(UTC) - timedelta(days=body.lookback_days)
        result = await session.execute(
            select(PriceHistory)
            .where(PriceHistory.pair_id == body.pair_id, PriceHistory.observed_at >= cutoff)
            .order_by(PriceHistory.observed_at.asc())
        )
        rows = result.scalars().all()

        # Notification.triggered_at is server_default=func.now() — Postgres's now() is
        # the TRANSACTION timestamp, constant for every statement in this one bulk
        # transaction, not the moment each individual row fired. Using it here would
        # report the same wall-clock instant for all N fires regardless of which
        # historical moment actually triggered them. row.observed_at, tracked directly
        # in this loop, is the real "when it happened" — that's what the response uses.
        all_fires: list[tuple[Notification, datetime]] = []
        for row in rows:
            created = await evaluate_rules_for_pair(
                session, pair, row.last_price, row.bid_price, row.ask_price, row.observed_at, rule_index
            )
            all_fires.extend((notification, row.observed_at) for notification in created)

        # No commit — closing the session below rolls back everything: temp_rule and
        # every Notification/OutboxEvent flushed during replay vanish with it.

        data_start = rows[0].observed_at if rows else None
        data_end = rows[-1].observed_at if rows else None
        span_days = (data_end - data_start).total_seconds() / 86400 if rows else 0.0

        fire_count = len(all_fires)
        post_cooldown_fire_count = sum(1 for n, _observed_at in all_fires if n.status == STATUS_PENDING)

        return BacktestResponse(
            pair_id=body.pair_id,
            rule_type=body.rule_type,
            lookback_days=body.lookback_days,
            observations_replayed=len(rows),
            data_start=data_start,
            data_end=data_end,
            fire_count=fire_count,
            fires_per_day=(fire_count / span_days) if span_days > 0 else 0.0,
            post_cooldown_fire_count=post_cooldown_fire_count,
            fires=[
                BacktestFireEvent(triggered_at=observed_at, detected_price=n.detected_price, status=n.status)
                for n, observed_at in all_fires
            ],
        )
