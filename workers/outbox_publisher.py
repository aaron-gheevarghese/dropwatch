"""Outbox publisher: polls pending OutboxEvent rows and publishes them to SNS.

Standard durable-outbox pattern: the rule evaluator (rules/evaluator.py) writes
Notification + OutboxEvent in the same DB transaction as the triggering PriceHistory
observation, so a fire is never lost even if SNS is briefly unreachable — this process
is the only thing that talks to SNS, decoupled from evaluation.

On success: records published_at on the OutboxEvent and status=sent/sent_at on the
Notification, in one commit. On failure: increments publish_attempts, records
last_error, and sets next_attempt_at to an exponential backoff (base * 2^(attempts-1),
capped) so a failing row doesn't retry on the very next tick and doesn't block healthy
rows behind it. Assumes scripts/setup_sns.py has already provisioned the topic — this
process only looks it up.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from db.client import async_session_factory
from db.models import Notification, OutboxEvent
from sns.client import get_sns_client, get_topic_arn

logger = logging.getLogger(__name__)

TICK_INTERVAL_SECONDS = 5
BATCH_SIZE = 20
SNS_SUBJECT_MAX_LENGTH = 100


def backoff_seconds(publish_attempts: int) -> int:
    return min(
        settings.outbox_backoff_base_seconds * (2 ** max(publish_attempts - 1, 0)),
        settings.outbox_backoff_max_seconds,
    )


async def _get_due_events(session: AsyncSession) -> list[OutboxEvent]:
    now = datetime.now(UTC)
    result = await session.execute(
        select(OutboxEvent)
        .where(
            OutboxEvent.published_at.is_(None),
            or_(OutboxEvent.next_attempt_at.is_(None), OutboxEvent.next_attempt_at <= now),
        )
        .order_by(OutboxEvent.created_at)
        .limit(BATCH_SIZE)
    )
    return list(result.scalars())


async def publish_pending(sns_client, topic_arn: str, session: AsyncSession) -> int:
    due = await _get_due_events(session)
    if not due:
        return 0

    now = datetime.now(UTC)
    published = 0

    for event in due:
        body = json.loads(event.payload)
        message = body.get("message", event.payload)
        subject = f"dropwatch alert: {body.get('pair_display_name', '')}"[:SNS_SUBJECT_MAX_LENGTH]

        try:
            await asyncio.to_thread(sns_client.publish, TopicArn=topic_arn, Subject=subject, Message=message)
        except Exception as exc:
            event.publish_attempts += 1
            event.last_error = str(exc)[:2000]
            event.next_attempt_at = now + timedelta(seconds=backoff_seconds(event.publish_attempts))
            logger.warning(
                "publish failed for outbox event %s (attempt %d): %s", event.id, event.publish_attempts, exc
            )
            continue

        event.published_at = now
        notification = await session.get(Notification, event.notification_id)
        notification.status = "sent"
        notification.sent_at = now
        published += 1

    await session.commit()
    return published


async def run_forever() -> None:
    sns_client = get_sns_client()
    topic_arn = await asyncio.to_thread(get_topic_arn, sns_client, settings.sns_topic_name)

    while True:
        async with async_session_factory() as session:
            try:
                await publish_pending(sns_client, topic_arn, session)
            except Exception:
                logger.exception("outbox publish tick failed")

        await asyncio.sleep(TICK_INTERVAL_SECONDS)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    await run_forever()


if __name__ == "__main__":
    asyncio.run(main())
