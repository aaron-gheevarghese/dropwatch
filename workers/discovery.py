"""Daily job: discover the tracked-pair universe from Kraken's full USD market.

Auto-creates a Pair row for any USD-quoted pair that clears the activate floor, and
applies hysteresis to pairs already tracked (by discovery or by POST /pairs) so a pair
oscillating between the two floors doesn't flap active/inactive across runs. Idempotent:
re-running against the same market data converges to the same state.
"""

import asyncio
import logging

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from db.client import async_session_factory
from db.models import Pair
from providers.base import MarketDataProvider
from providers.kraken import KrakenProvider

logger = logging.getLogger(__name__)


async def run_discovery(provider: MarketDataProvider, session: AsyncSession) -> None:
    snapshot = await provider.get_usd_market_snapshot()

    existing = await session.execute(select(Pair).where(Pair.kraken_pair_name.in_(snapshot.keys())))
    existing_by_name = {pair.kraken_pair_name: pair for pair in existing.scalars()}

    created = activated = deactivated = 0

    for canonical_name, market in snapshot.items():
        pair = existing_by_name.get(canonical_name)

        if pair is None:
            if market.notional_24h >= settings.discovery_activate_floor_usd:
                session.add(
                    Pair(
                        kraken_pair_name=market.canonical_name,
                        display_name=market.display_name,
                        base_currency=market.base_currency,
                        quote_currency=market.quote_currency,
                        poll_interval_seconds=settings.default_poll_interval_seconds,
                        is_active=True,
                        notional_24h=market.notional_24h,
                        current_last_price=market.last,
                        current_bid_price=market.bid,
                        current_ask_price=market.ask,
                    )
                )
                created += 1
            continue

        pair.notional_24h = market.notional_24h

        if market.notional_24h >= settings.discovery_activate_floor_usd and not pair.is_active:
            pair.is_active = True
            activated += 1
        elif market.notional_24h < settings.discovery_deactivate_floor_usd and pair.is_active:
            pair.is_active = False
            deactivated += 1
        # Between the two floors: leave is_active untouched (hysteresis band).

    await session.commit()
    logger.info(
        "discovery run complete: seen=%d created=%d activated=%d deactivated=%d",
        len(snapshot),
        created,
        activated,
        deactivated,
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    async with httpx.AsyncClient(timeout=10.0) as client:
        provider = KrakenProvider(
            client,
            base_url=settings.kraken_api_base_url,
            requests_per_second=settings.kraken_requests_per_second,
        )
        async with async_session_factory() as session:
            await run_discovery(provider, session)


if __name__ == "__main__":
    asyncio.run(main())
