from datetime import datetime
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from db.client import get_session
from db.models import AlertRule, Pair, User
from rules.evaluator import IMPLEMENTED_RULE_TYPES

router = APIRouter(prefix="/rules", tags=["rules"])


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
    def _validate_rule_type(self) -> "CreateRuleRequest":
        if self.rule_type not in IMPLEMENTED_RULE_TYPES:
            raise ValueError(
                f"rule_type must be one of {IMPLEMENTED_RULE_TYPES} for now "
                f"(percent_change, zscore_move, spread_widen land in Step 7)"
            )
        if self.threshold is None:
            raise ValueError(f"threshold is required for rule_type={self.rule_type!r}")
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
    return rule


@router.patch("/{rule_id}", response_model=RuleResponse)
async def update_rule(
    rule_id: UUID,
    body: UpdateRuleRequest,
    session: AsyncSession = Depends(get_session),
) -> AlertRule:
    rule = await session.get(AlertRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"rule {rule_id} not found")

    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(rule, field, value)

    if rule.rule_type in IMPLEMENTED_RULE_TYPES and rule.threshold is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"threshold is required for rule_type={rule.rule_type!r}",
        )

    await session.commit()
    await session.refresh(rule)
    return rule
