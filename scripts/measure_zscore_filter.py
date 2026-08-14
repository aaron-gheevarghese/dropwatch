"""Measures the zscore_move filter against the real captured Kraken history in
tests/fixtures/zscore_fixture.json. Prints a report and writes it to
docs/zscore_measurement_report.md.

Read this before trusting any number here — the first run of this script produced a
result that contradicted the naive "z-score should reduce false positives" assumption,
and digging into *why* changed what's actually reported:

1. **The literal PRD framing — total fires vs. one fixed naive threshold — is
   threshold-dependent and often unfavorable to z-score.** A |z| >= 2.0 cutoff has an
   inherent ~4.55% theoretical fire rate under a normal distribution — that's not a
   "conservative" setting, it's a fixed statistical significance level that doesn't
   care how loose or tight your naive comparison threshold happens to be. Against a
   naive threshold looser than z-score's effective rate, z-score "wins"; against a
   tighter one, it "loses". Real 60-second crypto returns are also fat-tailed (excess
   kurtosis), so z-score's actual empirical fire rate runs a bit above the Gaussian
   theoretical rate. Reported at four thresholds (0.1%/0.25%/0.5%/1%, calibrated to the
   fixture's actual observed move distribution, not guessed) rather than one
   cherry-picked value, precisely so this sensitivity is visible instead of hidden.

2. **The mechanism the PRD's theory actually rests on is per-pair consistency, not
   raw fire count, and that part measures out real and strong.** A fixed threshold
   applied uniformly across pairs fires on 0% to 74% of observations depending on the
   pair's own volatility (nearly every poll on a noisy thin pair, almost never on a
   calm one) — coefficient of variation ~1.6 across the 118 real pairs. z-score,
   self-calibrating to each pair's own recent volatility, fires in a tight 0.25%-13.6%
   band regardless of pair — coefficient of variation ~0.3, about 5x tighter. This is
   the actual, defensible value proposition: predictable alert rates regardless of
   which pair you're watching, not "fires less often" in some threshold-free sense.

3. Every real observation is treated as ground-truth "no genuine event happened"
   background noise — an assumption, not a confirmed fact (no external record of real
   news-driven moves exists). Sufficient for measuring false-positive RATE, not recall.

4. Synthetic edge cases (insufficient history, zero-variance, single-tick glitch) are
   validated separately as pass/fail correctness checks, not folded into either metric
   above — they're deliberately constructed, not a representative noise sample.

Usage: python -m scripts.measure_zscore_filter
"""

import json
import math
import statistics
from decimal import Decimal
from pathlib import Path

from workers.zscore_filter import compute_zscore

FIXTURE_PATH = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "zscore_fixture.json"
REPORT_PATH = Path(__file__).resolve().parent.parent / "docs" / "zscore_measurement_report.md"

SIGMA_CUTOFF = 2.0
# Calibrated against the fixture's actual move distribution: for liquid pairs
# (BTC/ETH/...), p95 of |nonzero move| is ~0.09% and p99 ~0.34%; across all 118 pairs
# (including thin ones) p95 is ~0.86% and p99 ~2.4%. These four span that range.
NAIVE_THRESHOLDS_PERCENT = (0.1, 0.25, 0.5, 1.0)
CONSISTENCY_THRESHOLD_PERCENT = 0.25
ZERO_VARIANCE_MIN_PERCENT = 0.5  # matches config/settings.py's default
WINDOW = 60
MIN_OBSERVATIONS = 30
MAX_PRICES_NEEDED = WINDOW + 2


def _sweep_pair(prices: list[Decimal], naive_threshold_percent: float) -> list[dict]:
    results = []
    for i in range(1, len(prices)):
        window_slice = prices[max(0, i + 1 - MAX_PRICES_NEEDED) : i + 1]
        zresult = compute_zscore(window_slice, window=WINDOW, min_observations=MIN_OBSERVATIONS)

        current, previous = float(prices[i]), float(prices[i - 1])
        naive_fires = abs((current - previous) / previous) * 100 > naive_threshold_percent

        if zresult is None:
            zscore_fires = False
            has_baseline = False
        elif zresult.is_zero_variance:
            percent_move = abs(math.expm1(zresult.current_return)) * 100
            zscore_fires = percent_move >= ZERO_VARIANCE_MIN_PERCENT
            has_baseline = True
        else:
            zscore_fires = abs(zresult.z) >= SIGMA_CUTOFF
            has_baseline = True

        results.append({"naive_fires": naive_fires, "zscore_fires": zscore_fires, "has_baseline": has_baseline})
    return results


def _reduction_percent(naive_count: int, zscore_count: int) -> float | None:
    if naive_count == 0:
        return None
    return (1 - zscore_count / naive_count) * 100


def run_synthetic_cases(cases: list[dict]) -> list[dict]:
    outcomes = []
    for case in cases:
        prices = [Decimal(p) for p in case["prices"]]
        if "next_price" in case:
            prices = prices + [Decimal(case["next_price"])]

        result = compute_zscore(prices, window=WINDOW, min_observations=MIN_OBSERVATIONS)

        if case["expected_zscore_result"] == "insufficient_history":
            passed = result is None
            detail = f"result={result}"
        elif case["expected_zscore_result"] == "zero_variance":
            passed = result is not None and result.is_zero_variance
            if passed and "expected_fires_at_default_config" in case:
                percent_move = abs(math.expm1(result.current_return)) * 100
                fires = percent_move >= ZERO_VARIANCE_MIN_PERCENT
                passed = fires == case["expected_fires_at_default_config"]
                detail = f"percent_move={percent_move:.3f}% fires={fires}"
            else:
                detail = f"is_zero_variance={result.is_zero_variance if result else None}"
        else:  # single_tick_glitch -- documents behavior, doesn't assert a specific outcome
            z = result.z if result and not result.is_zero_variance else None
            passed = True
            detail = f"z={z} (both methods fire on this — documented limitation, not solved)"

        outcomes.append({"name": case["name"], "passed": passed, "detail": detail})
    return outcomes


def run_reduction_measurement(real_pairs: list[dict]) -> dict:
    per_threshold: dict[float, dict] = {}
    for threshold in NAIVE_THRESHOLDS_PERCENT:
        naive_total = zscore_total = 0
        naive_total_excl = zscore_total_excl = 0

        for pair in real_pairs:
            prices = [Decimal(p) for p in pair["prices"]]
            sweep = _sweep_pair(prices, threshold)

            naive_total += sum(1 for r in sweep if r["naive_fires"])
            zscore_total += sum(1 for r in sweep if r["zscore_fires"])
            naive_total_excl += sum(1 for r in sweep if r["naive_fires"] and r["has_baseline"])
            zscore_total_excl += sum(1 for r in sweep if r["zscore_fires"] and r["has_baseline"])

        per_threshold[threshold] = {
            "naive_total": naive_total,
            "zscore_total": zscore_total,
            "reduction_percent_including_cold_start": _reduction_percent(naive_total, zscore_total),
            "reduction_percent_excluding_cold_start": _reduction_percent(naive_total_excl, zscore_total_excl),
        }
    return per_threshold


def run_consistency_measurement(real_pairs: list[dict]) -> dict:
    """The mechanism check: does each method's fire RATE stay consistent across pairs
    of very different volatility, or does it swing wildly depending on which pair?
    """
    naive_rates, zscore_rates, per_pair = [], [], []

    for pair in real_pairs:
        prices = [Decimal(p) for p in pair["prices"]]
        sweep = _sweep_pair(prices, CONSISTENCY_THRESHOLD_PERCENT)
        n = len(sweep)
        naive_n = sum(1 for r in sweep if r["naive_fires"])
        zscore_n = sum(1 for r in sweep if r["zscore_fires"])

        naive_rates.append(naive_n / n)
        zscore_rates.append(zscore_n / n)
        per_pair.append(
            {
                "pair_name": pair["pair_name"],
                "observations": n,
                "naive_fire_rate": naive_n / n,
                "zscore_fire_rate": zscore_n / n,
            }
        )

    per_pair.sort(key=lambda p: p["naive_fire_rate"], reverse=True)

    return {
        "naive_mean": statistics.mean(naive_rates),
        "naive_stdev": statistics.stdev(naive_rates),
        "naive_cv": statistics.stdev(naive_rates) / statistics.mean(naive_rates),
        "naive_min": min(naive_rates),
        "naive_max": max(naive_rates),
        "zscore_mean": statistics.mean(zscore_rates),
        "zscore_stdev": statistics.stdev(zscore_rates),
        "zscore_cv": statistics.stdev(zscore_rates) / statistics.mean(zscore_rates),
        "zscore_min": min(zscore_rates),
        "zscore_max": max(zscore_rates),
        "per_pair": per_pair,
    }


def format_report(fixture: dict, reduction: dict, consistency: dict, synthetic_outcomes: list[dict]) -> str:
    lines = ["# z-score filter measurement report", ""]
    lines.append(f"Generated from `{FIXTURE_PATH.name}` (built {fixture['generated_at']}).")
    lines.append(
        f"{len(fixture['real_pairs'])} real pairs, "
        f"{sum(len(p['prices']) for p in fixture['real_pairs'])} total real price observations, "
        f"sigma cutoff {SIGMA_CUTOFF}."
    )
    lines.append("")
    lines.append(
        "**Read the methodology docstring at the top of `scripts/measure_zscore_filter.py` "
        "in full before citing any number from this report** — the honest finding here is "
        "more nuanced than a single percentage."
    )
    lines.append("")

    lines.append("## 1. Per-pair consistency (the actual mechanism being validated)")
    lines.append("")
    lines.append(
        f"At a fixed {CONSISTENCY_THRESHOLD_PERCENT}% naive threshold, per-pair fire RATE "
        f"(fraction of observations that fire) across all {len(fixture['real_pairs'])} pairs:"
    )
    lines.append("")
    lines.append("| method | mean | stdev | coefficient of variation | min | max |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    lines.append(
        f"| naive @ {CONSISTENCY_THRESHOLD_PERCENT}% | {consistency['naive_mean']:.4f} | "
        f"{consistency['naive_stdev']:.4f} | {consistency['naive_cv']:.2f} | "
        f"{consistency['naive_min']:.4f} | {consistency['naive_max']:.4f} |"
    )
    lines.append(
        f"| z-score (sigma={SIGMA_CUTOFF}) | {consistency['zscore_mean']:.4f} | "
        f"{consistency['zscore_stdev']:.4f} | {consistency['zscore_cv']:.2f} | "
        f"{consistency['zscore_min']:.4f} | {consistency['zscore_max']:.4f} |"
    )
    lines.append("")
    cv_ratio = consistency["naive_cv"] / consistency["zscore_cv"]
    lines.append(
        f"**z-score's fire rate is ~{cv_ratio:.1f}x more consistent across pairs than a fixed "
        f"threshold's** (lower coefficient of variation = less swing between pairs). Naive "
        f"ranges from {consistency['naive_min']:.1%} to {consistency['naive_max']:.1%} of "
        f"observations firing depending on the pair; z-score stays within "
        f"{consistency['zscore_min']:.1%} to {consistency['zscore_max']:.1%} regardless of "
        f"the pair's own volatility. This is the real, measured value proposition."
    )
    lines.append("")

    lines.append("Pairs with the highest naive fire rate (where a fixed threshold spams hardest):")
    lines.append("")
    lines.append("| pair | naive fire rate | z-score fire rate |")
    lines.append("| --- | --- | --- |")
    for p in consistency["per_pair"][:8]:
        lines.append(f"| {p['pair_name']} | {p['naive_fire_rate']:.1%} | {p['zscore_fire_rate']:.1%} |")
    lines.append("")
    lines.append("Pairs with the lowest naive fire rate (where a fixed threshold would miss almost everything):")
    lines.append("")
    lines.append("| pair | naive fire rate | z-score fire rate |")
    lines.append("| --- | --- | --- |")
    for p in consistency["per_pair"][-8:]:
        lines.append(f"| {p['pair_name']} | {p['naive_fire_rate']:.1%} | {p['zscore_fire_rate']:.1%} |")
    lines.append("")

    lines.append("## 2. Literal false-positive reduction vs. a single fixed threshold")
    lines.append("")
    lines.append("This is the PRD's literal framing. It is threshold-dependent, and often negative:")
    lines.append("")
    lines.append("| naive threshold | naive fires | z-score fires | reduction (incl. cold-start) | reduction (excl. cold-start) |")
    lines.append("| --- | --- | --- | --- | --- |")
    for threshold, data in reduction.items():
        incl = data["reduction_percent_including_cold_start"]
        excl = data["reduction_percent_excluding_cold_start"]
        lines.append(
            f"| {threshold:.2f}% | {data['naive_total']} | {data['zscore_total']} | "
            f"{f'{incl:.1f}%' if incl is not None else 'n/a'} | {f'{excl:.1f}%' if excl is not None else 'n/a'} |"
        )
    lines.append("")
    lines.append(
        "z-score's absolute fire count doesn't change with the naive threshold (it doesn't use "
        "one). At the loosest threshold tested (0.1%, below the median real move), z-score fires "
        "less than naive. At every tighter, more realistic threshold, z-score fires MORE — "
        "because a 2-sigma cutoff has a fixed ~4.55%+ theoretical base rate that doesn't scale "
        "down just because a threshold is 'tight'. **No single percentage from this table should "
        "be quoted as 'the' false-positive reduction** — it depends entirely on what the naive "
        "threshold is compared against, which is exactly why section 1 is the more meaningful "
        "measurement."
    )
    lines.append("")

    lines.append("## 3. Synthetic edge-case correctness (not part of either metric above)")
    lines.append("")
    lines.append("| case | passed | detail |")
    lines.append("| --- | --- | --- |")
    for o in synthetic_outcomes:
        lines.append(f"| {o['name']} | {'yes' if o['passed'] else 'NO'} | {o['detail']} |")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text())
    real_pairs = fixture["real_pairs"]

    reduction = run_reduction_measurement(real_pairs)
    consistency = run_consistency_measurement(real_pairs)
    synthetic_outcomes = run_synthetic_cases(fixture["synthetic_cases"])

    report = format_report(fixture, reduction, consistency, synthetic_outcomes)
    print(report)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report)
    print(f"\n(also written to {REPORT_PATH})")


if __name__ == "__main__":
    main()
