import logging
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from db.client import get_session
from db.models import AlertRule, Pair, User
from rules.rule_types import IMPLEMENTED_RULE_TYPES

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

ZSCORE_DIRECTIONS = ("up", "down", "both")


def _validate_rule_fields(
    rule_type: str, threshold: Decimal | None, sigma: Decimal | None, direction: str | None
) -> None:
    if rule_type not in IMPLEMENTED_RULE_TYPES:
        raise ValueError(
            f"rule_type must be one of {IMPLEMENTED_RULE_TYPES} for now (percent_change, spread_widen remain Step 7)"
        )
    if rule_type in ("absolute_below", "absolute_above") and threshold is None:
        raise ValueError(f"threshold is required for rule_type={rule_type!r}")
    if rule_type == "zscore_move":
        if sigma is None:
            raise ValueError(f"sigma is required for rule_type={rule_type!r}")
        if direction is not None and direction not in ZSCORE_DIRECTIONS:
            raise ValueError(f"direction must be one of {ZSCORE_DIRECTIONS} for rule_type={rule_type!r}")


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
        _validate_rule_fields(self.rule_type, self.threshold, self.sigma, self.direction)
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
        _validate_rule_fields(rule.rule_type, rule.threshold, rule.sigma, rule.direction)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    await session.commit()
    await session.refresh(rule)
    await _publish_invalidation(redis_client, reason=f"rule {rule.id} updated")
    return rule
