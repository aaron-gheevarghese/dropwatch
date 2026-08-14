"""Scheduler: selects due pairs and enqueues one staggered SQS message per pair.

Due-pair selection reuses Step 1's is_active + poll_interval_seconds / last_checked_at
check. What's new is that a pair no longer gets polled inline — it gets a message,
staggered via SQS DelaySeconds across the poll cadence window so ~100-150 active pairs
don't all hit Kraken in the same instant. One pair per message, not batched: batching
the actual Ticker call is still the poller's job (Step 1's logic, unchanged).

Critically, last_checked_at is claimed at *enqueue* time, not left for the poller to set
on success. Messages are staggered up to poll_interval_seconds out, but the scheduler
ticks every 5s — without claiming eagerly, every tick before a message is actually
processed would still see the pair as due and enqueue it again, compounding without
bound. See enqueue_due_pairs for the reasoning on why a lost message doesn't need the
scheduler to retry it (that's what SQS redelivery/DLQ is for).
"""

import asyncio
import json
import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from db.client import async_session_factory
from db.models import Pair
from sqs.client import get_queue_url, get_sqs_client

logger = logging.getLogger(__name__)

TICK_INTERVAL_SECONDS = 5
MAX_SQS_DELAY_SECONDS = 900


async def get_due_pairs(session: AsyncSession) -> list[Pair]:
    now = datetime.now(UTC)

    result = await session.execute(select(Pair).where(Pair.is_active.is_(True)))
    active_pairs = result.scalars().all()

    return [
        pair
        for pair in active_pairs
        if pair.last_checked_at is None
        or (now - pair.last_checked_at).total_seconds() >= pair.poll_interval_seconds
    ]


def _stagger_delay_seconds(index: int, total: int, window_seconds: int) -> int:
    if total <= 1:
        return 0
    step = window_seconds / total
    return min(int(index * step), MAX_SQS_DELAY_SECONDS)


async def enqueue_due_pairs(sqs_client, queue_url: str, session: AsyncSession) -> int:
    due = await get_due_pairs(session)
    if not due:
        return 0

    claimed_at = datetime.now(UTC)

    for index, pair in enumerate(due):
        delay = _stagger_delay_seconds(index, len(due), settings.default_poll_interval_seconds)
        body = json.dumps({"pair_id": str(pair.id)})
        await asyncio.to_thread(
            sqs_client.send_message, QueueUrl=queue_url, MessageBody=body, DelaySeconds=delay
        )
        # Claim it now, not when the poller eventually succeeds: messages are staggered
        # up to poll_interval_seconds out, so without this the next 5s tick would still
        # see a just-enqueued pair as "due" (last_checked_at is still stale) and enqueue
        # it again — every tick, compounding without bound. If this specific message is
        # lost, that's SQS's redelivery/DLQ's job, not the scheduler's; the pair just
        # waits for its next natural cadence.
        pair.last_checked_at = claimed_at

    await session.commit()

    logger.info(
        "enqueued %d due pairs, staggered over %ds", len(due), settings.default_poll_interval_seconds
    )
    return len(due)


async def run_forever() -> None:
    sqs_client = get_sqs_client()
    queue_url = await asyncio.to_thread(get_queue_url, sqs_client, settings.sqs_poll_queue_name)

    while True:
        async with async_session_factory() as session:
            try:
                await enqueue_due_pairs(sqs_client, queue_url, session)
            except Exception:
                logger.exception("scheduler tick failed")

        await asyncio.sleep(TICK_INTERVAL_SECONDS)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    await run_forever()


if __name__ == "__main__":
    asyncio.run(main())
