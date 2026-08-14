"""Builds the version-controlled labeled fixture dataset the PRD calls for: real
captured Kraken price history (for the honest false-positive-reduction measurement)
plus explicitly constructed synthetic edge cases (insufficient history, zero-variance,
a single-tick glitch) for correctness testing.

Real data note: every real observation here is treated as ground-truth "background,
normal market activity" — not a confirmed absence of news events, but the best available
label without manual annotation. This is sufficient for measuring FALSE POSITIVE rate
specifically (how often each method fires on data with no known reason to fire); it says
nothing about either method's true-positive/recall rate, which isn't what's being
measured here (see scripts/measure_zscore_filter.py for the methodology writeup).

Usage: python -m scripts.build_zscore_fixture
"""

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select

from db.client import async_session_factory
from db.models import Pair, PriceHistory

logger = logging.getLogger(__name__)

FIXTURE_PATH = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "zscore_fixture.json"
MIN_ROWS_PER_PAIR = 500


async def _fetch_real_pairs() -> list[dict]:
    async with async_session_factory() as session:
        pairs = (await session.execute(select(Pair))).scalars().all()

        real_pairs = []
        for pair in pairs:
            result = await session.execute(
                select(PriceHistory.last_price, PriceHistory.observed_at)
                .where(PriceHistory.pair_id == pair.id)
                .order_by(PriceHistory.observed_at.asc())
            )
            rows = result.all()
            if len(rows) < MIN_ROWS_PER_PAIR:
                continue

            real_pairs.append(
                {
                    "pair_name": pair.kraken_pair_name,
                    "display_name": pair.display_name,
                    "prices": [str(price) for price, _observed_at in rows],
                    "observed_at": [observed_at.astimezone(UTC).isoformat() for _price, observed_at in rows],
                }
            )
        return real_pairs


def _build_synthetic_cases() -> list[dict]:
    cases = []

    # Insufficient history: fewer than 32 observations total. Neither method should be
    # able to claim statistical confidence here regardless of the move size.
    prices = [str(round(100 * (1.001**i), 4)) for i in range(20)]
    cases.append(
        {
            "name": "insufficient_history",
            "description": "Only 20 observations (< 32 needed for a 30-return baseline) ending in a real move.",
            "prices": prices + ["110.0000"],
            "expected_zscore_result": "insufficient_history",
        }
    )

    # Zero-variance: a perfectly flat window (e.g. an illiquid pair with no trades
    # between polls, so the same last-traded price repeats) followed by a real move.
    flat = ["100.0000"] * 35
    cases.append(
        {
            "name": "zero_variance_small_move_below_floor",
            "description": "35 identical prices, then a 0.2% move — below the configured min-percent floor.",
            "prices": flat,
            "next_price": "100.2000",
            "expected_zscore_result": "zero_variance",
            "expected_fires_at_default_config": False,
        }
    )
    cases.append(
        {
            "name": "zero_variance_large_move_above_floor",
            "description": "35 identical prices, then a 2% move — above the configured min-percent floor.",
            "prices": flat,
            "next_price": "102.0000",
            "expected_zscore_result": "zero_variance",
            "expected_fires_at_default_config": True,
        }
    )

    # Single-tick glitch: one wildly-off observation that immediately reverts. Neither
    # a naive raw-delta filter nor a single-observation z-score can structurally tell
    # this apart from a genuine spike — both are expected to fire on the glitch tick
    # itself. This case documents that limitation rather than claiming to solve it;
    # see the measurement report for why it's excluded from the headline number.
    calm = [str(round(100 * (1 + 0.0005 * ((-1) ** i)), 4)) for i in range(40)]
    glitch_sequence = calm + ["150.0000", "100.4000"]  # spike, then reverts near baseline
    cases.append(
        {
            "name": "single_tick_glitch_spike_and_revert",
            "description": "A 40-observation calm baseline, one wild outlier tick, then reversion.",
            "prices": glitch_sequence,
            "expected_zscore_result": "not_directly_solved",
        }
    )

    return cases


async def build_fixture() -> dict:
    real_pairs = await _fetch_real_pairs()
    synthetic_cases = _build_synthetic_cases()

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "real Kraken price_history captured via the dropwatch poller against the live Supabase dev DB",
        "min_rows_per_pair": MIN_ROWS_PER_PAIR,
        "real_pairs": real_pairs,
        "synthetic_cases": synthetic_cases,
    }


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    fixture = await build_fixture()

    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE_PATH.write_text(json.dumps(fixture, indent=2))

    logger.info(
        "wrote %s: %d real pairs, %d synthetic cases",
        FIXTURE_PATH,
        len(fixture["real_pairs"]),
        len(fixture["synthetic_cases"]),
    )


if __name__ == "__main__":
    asyncio.run(main())
