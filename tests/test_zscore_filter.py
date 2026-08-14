"""Pure-logic tests for workers/zscore_filter.py — no DB, no async."""

import math
import statistics
from decimal import Decimal

from workers.zscore_filter import compute_zscore


def _prices(values: list[float]) -> list[Decimal]:
    return [Decimal(str(v)) for v in values]


def test_fewer_than_two_prices_returns_none() -> None:
    assert compute_zscore(_prices([100])) is None
    assert compute_zscore(_prices([])) is None


def test_below_min_observations_returns_none() -> None:
    # 30 min_observations needs 32 prices minimum (30 baseline returns + the pair of
    # prices forming the current return). 31 prices -> only 29 baseline returns -> None.
    prices = _prices([100.0 + i * 0.01 for i in range(31)])
    assert compute_zscore(prices, window=60, min_observations=30) is None


def test_exactly_at_minimum_observations_computes() -> None:
    prices = _prices([100.0 + i * 0.01 for i in range(32)])
    result = compute_zscore(prices, window=60, min_observations=30)
    assert result is not None
    assert result.window_size == 30


def test_known_zscore_matches_hand_computed_value() -> None:
    # A flat baseline with small random-ish jitter (not perfectly constant, so variance
    # is real and nonzero), then a clearly anomalous final jump.
    import random

    rng = random.Random(42)
    baseline_prices = [100.0]
    for _ in range(40):
        baseline_prices.append(baseline_prices[-1] * (1 + rng.uniform(-0.001, 0.001)))
    # A sharp final move, clearly outside the baseline's tiny jitter.
    final_price = baseline_prices[-1] * 1.05

    prices = _prices(baseline_prices + [final_price])
    result = compute_zscore(prices, window=60, min_observations=30)
    assert result is not None
    assert not result.is_zero_variance

    # Cross-check against a plain, independent computation of the same formula.
    floats = [float(p) for p in prices]
    returns = [math.log(floats[i] / floats[i - 1]) for i in range(1, len(floats))]
    baseline = returns[:-1]
    expected_z = (returns[-1] - statistics.fmean(baseline)) / statistics.stdev(baseline)

    assert result.z == expected_z
    assert result.z > 3, "a 5% jump against ~0.1%-jitter baseline should be a large positive z"


def test_zero_variance_window_flags_instead_of_dividing_by_zero() -> None:
    # A perfectly flat baseline (every price identical) followed by a real move.
    prices = _prices([100.0] * 35 + [101.0])
    result = compute_zscore(prices, window=60, min_observations=30)
    assert result is not None
    assert result.is_zero_variance is True
    assert result.z is None
    assert result.current_return > 0


def test_window_caps_to_most_recent_n_baseline_returns() -> None:
    # A wild early segment, then a long calm tail, then the current move. If the window
    # correctly caps to the most recent 60 baseline returns, the wild early prices
    # should have zero influence on the result.
    wild_early = [100.0, 200.0, 50.0, 300.0, 10.0]  # would blow up mean/stdev if included
    calm_tail = [100.0]
    for i in range(65):
        calm_tail.append(calm_tail[-1] * 1.0001)  # tiny, near-constant drift
    current = calm_tail[-1] * 1.02

    full_sequence = _prices(wild_early + calm_tail + [current])
    capped_result = compute_zscore(full_sequence, window=60, min_observations=30)

    tail_only_sequence = _prices(calm_tail + [current])
    tail_only_result = compute_zscore(tail_only_sequence, window=60, min_observations=30)

    assert capped_result is not None
    assert tail_only_result is not None
    assert capped_result.window_size == 60
    assert capped_result.z == tail_only_result.z, "wild early prices outside the window must not affect the result"
