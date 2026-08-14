from datetime import datetime
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.client import get_session
from db.models import AlertRule, Notification, OutboxEvent, Pair

router = APIRouter(prefix="/alerts", tags=["alerts"])


class AlertResponse(BaseModel):
    id: UUID
    rule_id: UUID
    pair_id: UUID
    pair_display_name: str
    rule_type: str
    threshold: Decimal | None
    detected_price: Decimal
    status: str
    triggered_at: datetime
    sent_at: datetime | None

    # Outbox delivery diagnostics — null for suppressed notifications, which never get
    # an OutboxEvent. This is the main debugging surface: why hasn't this alert gone
    # out, how many times has delivery been retried, when's the next attempt.
    publish_attempts: int | None
    last_error: str | None
    next_attempt_at: datetime | None
    published_at: datetime | None


@router.get("", response_model=list[AlertResponse])
async def list_alerts(
    pair_id: UUID | None = None,
    rule_id: UUID | None = None,
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> list[AlertResponse]:
    stmt = (
        select(Notification, Pair.display_name, AlertRule.threshold, OutboxEvent)
        .join(Pair, Pair.id == Notification.pair_id)
        .join(AlertRule, AlertRule.id == Notification.rule_id)
        .outerjoin(OutboxEvent, OutboxEvent.notification_id == Notification.id)
        .order_by(Notification.triggered_at.desc())
        .limit(limit)
        .offset(offset)
    )

    if pair_id is not None:
        stmt = stmt.where(Notification.pair_id == pair_id)
    if rule_id is not None:
        stmt = stmt.where(Notification.rule_id == rule_id)
    if status is not None:
        stmt = stmt.where(Notification.status == status)

    rows = await session.execute(stmt)

    return [
        AlertResponse(
            id=notification.id,
            rule_id=notification.rule_id,
            pair_id=notification.pair_id,
            pair_display_name=pair_display_name,
            rule_type=notification.type,
            threshold=threshold,
            detected_price=notification.detected_price,
            status=notification.status,
            triggered_at=notification.triggered_at,
            sent_at=notification.sent_at,
            publish_attempts=outbox_event.publish_attempts if outbox_event else None,
            last_error=outbox_event.last_error if outbox_event else None,
            next_attempt_at=outbox_event.next_attempt_at if outbox_event else None,
            published_at=outbox_event.published_at if outbox_event else None,
        )
        for notification, pair_display_name, threshold, outbox_event in rows.all()
    ]
