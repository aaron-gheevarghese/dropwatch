"""SQS consumer: receives poll messages, does the batched Ticker call + PriceHistory
write from Step 1's logic, and deletes each message only once its pair's observation is
committed. A message left undeleted becomes visible again after the queue's visibility
timeout and gets redelivered — after sqs_max_receive_count receives, the redrive policy
set up by scripts/setup_sqs.py moves it to the DLQ automatically. Still no
interpolation: a pair that fails, or that Kraken simply omits from the response, is
left alone rather than written with a guessed value.

After each successful PriceHistory write, absolute_below/absolute_above rules for that
pair are evaluated in the same transaction (rules/evaluator.py) — a qualifying rule
creates a Notification + OutboxEvent that commits atomically with the observation itself.
"""

import asyncio
import json
import logging
from datetime import UTC, datetime
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from db.client import async_session_factory
from db.models import Pair, PriceHistory
from providers.base import MarketDataProvider
from providers.kraken import KrakenProvider
from rules.evaluator import evaluate_rules_for_pair
from sqs.client import get_queue_url, get_sqs_client

logger = logging.getLogger(__name__)

RECEIVE_MAX_MESSAGES = 10
RECEIVE_WAIT_TIME_SECONDS = 10


async def _receive(sqs_client, queue_url: str) -> list[dict]:
    response = await asyncio.to_thread(
        sqs_client.receive_message,
        QueueUrl=queue_url,
        MaxNumberOfMessages=RECEIVE_MAX_MESSAGES,
        WaitTimeSeconds=RECEIVE_WAIT_TIME_SECONDS,
    )
    return response.get("Messages", [])


async def _delete_batch(sqs_client, queue_url: str, messages: list[dict]) -> None:
    if not messages:
        return
    entries = [{"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]} for message in messages]
    await asyncio.to_thread(sqs_client.delete_message_batch, QueueUrl=queue_url, Entries=entries)


def _parse_pair_id(message: dict) -> UUID | None:
    try:
        return UUID(json.loads(message["Body"])["pair_id"])
    except (KeyError, ValueError, json.JSONDecodeError):
        logger.warning("malformed message %s, leaving for redelivery", message.get("MessageId"))
        return None


async def process_batch(
    provider: MarketDataProvider,
    session: AsyncSession,
    sqs_client,
    queue_url: str,
    messages: list[dict],
) -> None:
    by_pair_id: dict[UUID, dict] = {}
    for message in messages:
        pair_id = _parse_pair_id(message)
        if pair_id is not None:
            by_pair_id[pair_id] = message

    if not by_pair_id:
        return

    result = await session.execute(select(Pair).where(Pair.id.in_(by_pair_id.keys())))
    pairs_by_id = {pair.id: pair for pair in result.scalars()}

    to_delete = []

    # A message for a pair that's been deleted since it was enqueued has nothing to
    # retry — drop it rather than let it redeliver forever.
    for pair_id, message in by_pair_id.items():
        if pair_id not in pairs_by_id:
            logger.warning("pair %s no longer exists; dropping message", pair_id)
            to_delete.append(message)

    live_pair_ids = [pid for pid in by_pair_id if pid in pairs_by_id]
    if not live_pair_ids:
        await _delete_batch(sqs_client, queue_url, to_delete)
        return

    message_by_canonical = {pairs_by_id[pid].kraken_pair_name: by_pair_id[pid] for pid in live_pair_ids}
    pair_by_canonical = {pairs_by_id[pid].kraken_pair_name: pairs_by_id[pid] for pid in live_pair_ids}
    canonical_names = list(message_by_canonical.keys())

    try:
        quotes = await provider.get_quotes(canonical_names)
    except Exception:
        logger.exception("Ticker batch failed for %d pairs; leaving for redelivery", len(canonical_names))
        await _delete_batch(sqs_client, queue_url, to_delete)
        return

    observed_at = datetime.now(UTC)

    for canonical_name, message in message_by_canonical.items():
        quote = quotes.get(canonical_name)
        if quote is None:
            logger.warning("no ticker returned for %s; leaving for redelivery", canonical_name)
            continue

        pair = pair_by_canonical[canonical_name]

        session.add(
            PriceHistory(
                pair_id=pair.id,
                last_price=quote.last,
                bid_price=quote.bid,
                ask_price=quote.ask,
                volume_24h=quote.volume_24h,
                observed_at=observed_at,
            )
        )
        pair.current_last_price = quote.last
        pair.current_bid_price = quote.bid
        pair.current_ask_price = quote.ask
        pair.last_checked_at = observed_at
        to_delete.append(message)

        await evaluate_rules_for_pair(session, pair, quote.last, observed_at)

    await session.commit()
    await _delete_batch(sqs_client, queue_url, to_delete)


async def run_forever(provider: MarketDataProvider) -> None:
    sqs_client = get_sqs_client()
    queue_url = await asyncio.to_thread(get_queue_url, sqs_client, settings.sqs_poll_queue_name)

    while True:
        messages = await _receive(sqs_client, queue_url)
        if not messages:
            continue

        async with async_session_factory() as session:
            try:
                await process_batch(provider, session, sqs_client, queue_url, messages)
            except Exception:
                logger.exception("poll batch processing failed")


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    async with httpx.AsyncClient(timeout=10.0) as client:
        provider = KrakenProvider(
            client,
            base_url=settings.kraken_api_base_url,
            requests_per_second=settings.kraken_requests_per_second,
        )
        await run_forever(provider)


if __name__ == "__main__":
    asyncio.run(main())
