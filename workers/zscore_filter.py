"""Core z-score-over-log-returns computation. Pure functions, no DB/IO — the caller
(rules/evaluator.py) is responsible for fetching the price window and deciding what to
do with the result.

Per the PRD:
  r_t = ln(price_t / price_{t-1})
  z = (r_t - mean(r_window)) / stdev(r_window)

r_t is scored against a window of the PRECEDING returns, not including itself — folding
r_t into its own baseline would bias the baseline toward the very value being tested
(with only 60 samples, one outlier measurably drags the sample mean/stdev toward it),
which defeats the point of an anomaly score. This is a standard out-of-sample scoring
choice, not a PRD-specified detail.

Edge cases handled here (the rest — cooldown, idempotency — are rules/evaluator.py's job):
  - Fewer than MIN_OBSERVATIONS (30) prior returns: returns None, meaning "can't score
    yet" — the caller must treat this as "does not fire", not as a zero/neutral z-score.
  - Zero-variance window (every return in the baseline is identical, e.g. a totally flat
    or extremely thin-trading period): division by zero is undefined, so this is flagged
    via is_zero_variance rather than computing z. The caller falls back to a configurable
    minimum-percent-change check for this case.
  - No interpolation: this module only ever sees the real price sequence the caller
    fetched from PriceHistory. It has no concept of "expected" timestamps or synthetic
    fill values — a gap between two real observations (a skipped poll) just becomes one
    return computed over however much wall-clock time actually elapsed. Nothing here
    fabricates a row to smooth that over.
"""

import math
import statistics
from dataclasses import dataclass
from decimal import Decimal

DEFAULT_WINDOW = 60
DEFAULT_MIN_OBSERVATIONS = 30


@dataclass(frozen=True)
class ZScoreResult:
    current_return: float
    window_size: int
    is_zero_variance: bool
    z: float | None  # None when is_zero_variance is True


def compute_zscore(
    prices: list[Decimal],
    window: int = DEFAULT_WINDOW,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
) -> ZScoreResult | None:
    """`prices` must be chronologically ordered, oldest first, with the price being
    scored as the last element. Returns None if there aren't enough prior observations
    to form a baseline (fewer than `min_observations` preceding returns).
    """
    if len(prices) < 2:
        return None

    floats = [float(p) for p in prices]
    returns = [math.log(floats[i] / floats[i - 1]) for i in range(1, len(floats))]

    current_return = returns[-1]
    baseline_all = returns[:-1]

    if len(baseline_all) < min_observations:
        return None

    baseline = baseline_all[-window:] if len(baseline_all) > window else baseline_all

    mean = statistics.fmean(baseline)
    # Sample stdev (n-1 denominator): the baseline is a sample used to estimate the
    # pair's underlying return volatility, not the full population of its returns.
    stdev = statistics.stdev(baseline) if len(baseline) >= 2 else 0.0

    if stdev == 0.0:
        return ZScoreResult(current_return=current_return, window_size=len(baseline), is_zero_variance=True, z=None)

    z = (current_return - mean) / stdev
    return ZScoreResult(current_return=current_return, window_size=len(baseline), is_zero_variance=False, z=z)
