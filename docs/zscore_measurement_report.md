# z-score filter measurement report

Generated from `zscore_fixture.json` (built 2026-08-14T21:38:53.630951+00:00).
118 real pairs, 142330 total real price observations, sigma cutoff 2.0.

**Read the methodology docstring at the top of `scripts/measure_zscore_filter.py` in full before citing any number from this report** — the honest finding here is more nuanced than a single percentage.

## 1. Per-pair consistency (the actual mechanism being validated)

At a fixed 0.25% naive threshold, per-pair fire RATE (fraction of observations that fire) across all 118 pairs:

| method | mean | stdev | coefficient of variation | min | max |
| --- | --- | --- | --- | --- | --- |
| naive @ 0.25% | 0.0763 | 0.1211 | 1.59 | 0.0000 | 0.7442 |
| z-score (sigma=2.0) | 0.0657 | 0.0191 | 0.29 | 0.0033 | 0.1358 |

**z-score's fire rate is ~5.5x more consistent across pairs than a fixed threshold's** (lower coefficient of variation = less swing between pairs). Naive ranges from 0.0% to 74.4% of observations firing depending on the pair; z-score stays within 0.3% to 13.6% regardless of the pair's own volatility. This is the real, measured value proposition.

Pairs with the highest naive fire rate (where a fixed threshold spams hardest):

| pair | naive fire rate | z-score fire rate |
| --- | --- | --- |
| AKEUSD | 74.4% | 6.4% |
| SCRTUSD | 51.2% | 5.3% |
| USUSD | 47.6% | 6.9% |
| CAPUSD | 46.7% | 7.7% |
| VELVETUSD | 41.1% | 7.4% |
| ACUUSD | 37.4% | 6.1% |
| APRUSD | 31.2% | 6.8% |
| GWEIUSD | 29.3% | 7.3% |

Pairs with the lowest naive fire rate (where a fixed threshold would miss almost everything):

| pair | naive fire rate | z-score fire rate |
| --- | --- | --- |
| ZGBPZUSD | 0.0% | 5.2% |
| USDCUSD | 0.0% | 0.3% |
| XXBTZUSD | 0.0% | 6.8% |
| USDGUSD | 0.0% | 13.6% |
| SOLUSD | 0.0% | 6.2% |
| AUDUSD | 0.0% | 8.0% |
| BNBUSD | 0.0% | 6.4% |
| TRXUSD | 0.0% | 6.1% |

## 2. Literal false-positive reduction vs. a single fixed threshold

This is the PRD's literal framing. It is threshold-dependent, and often negative:

| naive threshold | naive fires | z-score fires | reduction (incl. cold-start) | reduction (excl. cold-start) |
| --- | --- | --- | --- | --- |
| 0.10% | 20540 | 9348 | 54.5% | 53.1% |
| 0.25% | 10838 | 9348 | 13.7% | 11.0% |
| 0.50% | 5257 | 9348 | -77.8% | -83.8% |
| 1.00% | 2001 | 9348 | -367.2% | -381.9% |

z-score's absolute fire count doesn't change with the naive threshold (it doesn't use one). At the loosest threshold tested (0.1%, below the median real move), z-score fires less than naive. At every tighter, more realistic threshold, z-score fires MORE — because a 2-sigma cutoff has a fixed ~4.55%+ theoretical base rate that doesn't scale down just because a threshold is 'tight'. **No single percentage from this table should be quoted as 'the' false-positive reduction** — it depends entirely on what the naive threshold is compared against, which is exactly why section 1 is the more meaningful measurement.

## 3. Synthetic edge-case correctness (not part of either metric above)

| case | passed | detail |
| --- | --- | --- |
| insufficient_history | yes | result=None |
| zero_variance_small_move_below_floor | yes | percent_move=0.200% fires=False |
| zero_variance_large_move_above_floor | yes | percent_move=2.000% fires=True |
| single_tick_glitch_spike_and_revert | yes | z=-6.411113991897137 (both methods fire on this — documented limitation, not solved) |
