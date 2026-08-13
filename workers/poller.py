"""Standalone async polling loop (no SQS yet).

Every tick, polls whichever active pairs are due (now - last_checked_at >=
poll_interval_seconds), batches them into one Ticker call, and writes PriceHistory for
each pair that came back. A pair that fails — whether the whole batch request errors, or
Kraken simply omits that pair from the response — is skipped for this cycle: no row is
written and no value is interpolated. It will be retried next tick.
"""

import asyncio
import logging
from datetime import UTC, datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from db.client import async_session_factory
from db.models import Pair, PriceHistory
from providers.base import MarketDataProvider
from providers.kraken import KrakenProvider

logger = logging.getLogger(__name__)

TICK_INTERVAL_SECONDS = 5


async def poll_due_pairs(provider: MarketDataProvider, session: AsyncSession) -> None:
    now = datetime.now(UTC)

    result = await session.execute(select(Pair).where(Pair.is_active.is_(True)))
    active_pairs = result.scalars().all()

    due = [
        pair
        for pair in active_pairs
        if pair.last_checked_at is None
        or (now - pair.last_checked_at).total_seconds() >= pair.poll_interval_seconds
    ]
    if not due:
        return

    by_canonical = {pair.kraken_pair_name: pair for pair in due}

    try:
        quotes = await provider.get_quotes(list(by_canonical.keys()))
    except Exception:
        logger.exception("poll batch failed for %d pairs; skipping this cycle", len(due))
        return

    checked_at = datetime.now(UTC)

    for canonical_name, pair in by_canonical.items():
        quote = quotes.get(canonical_name)
        if quote is None:
            logger.warning("no ticker returned for %s; skipping", canonical_name)
            continue

        session.add(
            PriceHistory(
                pair_id=pair.id,
                last_price=quote.last,
                bid_price=quote.bid,
                ask_price=quote.ask,
                checked_at=checked_at,
            )
        )
        pair.current_last_price = quote.last
        pair.current_bid_price = quote.bid
        pair.current_ask_price = quote.ask
        pair.last_checked_at = checked_at

    await session.commit()


async def run_forever(provider: MarketDataProvider) -> None:
    while True:
        async with async_session_factory() as session:
            try:
                await poll_due_pairs(provider, session)
            except Exception:
                logger.exception("poll cycle failed")

        await asyncio.sleep(TICK_INTERVAL_SECONDS)


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
