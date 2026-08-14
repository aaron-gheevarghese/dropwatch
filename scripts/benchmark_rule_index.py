"""Measures naive per-pair DB-query latency vs. the in-memory RuleIndex, at the PRD's
own example scale (~120 pairs, up to 400 rules/pair), against real Postgres — not an
assumed number. Prints a report and writes it to docs/rule_index_benchmark_report.md.

Methodology, same standard as Step 5's z-score validation:

- "Naive" replicates the exact query rules/evaluator.py used before Step 6: one
  `SELECT ... WHERE pair_id = X AND is_enabled AND rule_type IN (...)` per pair, timed
  for real against the live Supabase pooler — this is what production actually paid,
  not a synthetic proxy for it.
- "Indexed" is rule_index.rules_for_pair() (an O(1) dict lookup) followed by the same
  short-circuit scan rules/evaluator.py runs in production, using a fixed evaluation
  price chosen to sit mid-distribution (so the scan does a realistic amount of
  comparison work, not a trivial all-match or no-match).
- The speedup is expected to be dominated by eliminating a network round-trip per pair,
  not by algorithmic complexity — Postgres can scan even 400 indexed rows in
  sub-millisecond CPU time. Both the total per-cycle time (what actually matters: 120
  pairs evaluated once) and the raw per-call latency are reported, so that's visible
  rather than hidden behind one ratio.
- The one-time index rebuild cost (query all rules, bucket, sort) is reported
  separately — it's not on the poller's hot path, but it's a real cost on startup and
  on every invalidation, worth knowing.
- All synthetic data (pairs, user, rules) is created fresh, isolated by a unique run
  prefix, and fully deleted at the end regardless of outcome.

Usage: python -m scripts.benchmark_rule_index
"""

import asyncio
import statistics
import time
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from sqlalchemy import delete, insert, select

from db.client import async_session_factory
from db.models import AlertRule, Pair, User
from rules.evaluator import _matching_absolute_rules
from workers.rule_index import RuleIndex

REPORT_PATH = Path(__file__).resolve().parent.parent / "docs" / "rule_index_benchmark_report.md"

NUM_PAIRS = 120
RULES_PER_PAIR_SCALES = (10, 100, 400)  # 400 is the PRD's stated worst case
EVALUATION_PRICE = Decimal("500000")  # mid-distribution; thresholds spread 1..1_000_000


async def _setup_pairs_and_user(run_id: str) -> tuple[list, object]:
    async with async_session_factory() as session:
        user = User(contact=f"benchmark-{run_id}@example.com")
        session.add(user)
        pairs = [
            Pair(
                kraken_pair_name=f"BENCH{run_id}{i:04d}USD",
                display_name=f"BENCH{i}/USD",
                base_currency=f"BENCH{i}",
                quote_currency="USD",
                poll_interval_seconds=60,
                is_active=False,
            )
            for i in range(NUM_PAIRS)
        ]
        session.add_all(pairs)
        await session.commit()
        for pair in pairs:
            await session.refresh(pair)
        await session.refresh(user)
        return pairs, user


async def _insert_synthetic_rules(pairs: list, user, rules_per_pair: int) -> None:
    # Thresholds spread widely (1 to 1,000,000) so the short-circuit scan at
    # EVALUATION_PRICE does realistic, non-trivial work rather than matching everything
    # or nothing immediately.
    rows = []
    for pair in pairs:
        for j in range(rules_per_pair):
            threshold = Decimal(1 + (j * 1_000_000) // max(rules_per_pair, 1))
            rule_type = "absolute_below" if j % 2 == 0 else "absolute_above"
            rows.append(
                {
                    "id": uuid4(),
                    "user_id": user.id,
                    "pair_id": pair.id,
                    "rule_type": rule_type,
                    "threshold": threshold,
                    "cooldown_seconds": 0,
                    "is_enabled": True,
                }
            )

    async with async_session_factory() as session:
        # Bulk Core insert, not ORM session.add per row — 120 * 400 = 48,000 rows.
        for i in range(0, len(rows), 5000):
            await session.execute(insert(AlertRule), rows[i : i + 5000])
        await session.commit()


async def _delete_all_rules_for_pairs(pair_ids: list) -> None:
    async with async_session_factory() as session:
        await session.execute(delete(AlertRule).where(AlertRule.pair_id.in_(pair_ids)))
        await session.commit()


async def _cleanup(pairs: list, user) -> None:
    async with async_session_factory() as session:
        for pair in pairs:
            pair_row = await session.get(Pair, pair.id)
            if pair_row is not None:
                await session.delete(pair_row)  # cascades to any remaining rules
        user_row = await session.get(User, user.id)
        if user_row is not None:
            await session.delete(user_row)
        await session.commit()


def _percentile(values: list[float], p: float) -> float:
    ordered = sorted(values)
    idx = min(int(len(ordered) * p), len(ordered) - 1)
    return ordered[idx]


async def _benchmark_naive(pairs: list) -> list[float]:
    latencies = []
    async with async_session_factory() as session:
        for pair in pairs:
            start = time.perf_counter()
            await session.execute(
                select(AlertRule).where(
                    AlertRule.pair_id == pair.id,
                    AlertRule.is_enabled.is_(True),
                    AlertRule.rule_type.in_(("absolute_below", "absolute_above", "zscore_move")),
                )
            )
            latencies.append(time.perf_counter() - start)
    return latencies


def _benchmark_indexed(index: RuleIndex, pairs: list) -> list[float]:
    latencies = []
    for pair in pairs:
        start = time.perf_counter()
        bucket = index.rules_for_pair(pair.id)
        _matching_absolute_rules(bucket.absolute_below, EVALUATION_PRICE)
        _matching_absolute_rules(bucket.absolute_above, EVALUATION_PRICE)
        latencies.append(time.perf_counter() - start)
    return latencies


def _summarize(latencies: list[float]) -> dict:
    return {
        "total_ms": sum(latencies) * 1000,
        "mean_ms": statistics.mean(latencies) * 1000,
        "p50_ms": _percentile(latencies, 0.50) * 1000,
        "p95_ms": _percentile(latencies, 0.95) * 1000,
    }


async def run_benchmark() -> dict:
    run_id = uuid4().hex[:8].upper()
    pairs, user = await _setup_pairs_and_user(run_id)
    pair_ids = [p.id for p in pairs]

    results = {}
    try:
        for rules_per_pair in RULES_PER_PAIR_SCALES:
            await _insert_synthetic_rules(pairs, user, rules_per_pair)

            naive_latencies = await _benchmark_naive(pairs)

            rebuild_start = time.perf_counter()
            index = RuleIndex()
            rule_count = await index.rebuild()
            rebuild_ms = (time.perf_counter() - rebuild_start) * 1000

            indexed_latencies = _benchmark_indexed(index, pairs)

            naive_summary = _summarize(naive_latencies)
            indexed_summary = _summarize(indexed_latencies)

            results[rules_per_pair] = {
                "rule_count_indexed": rule_count,
                "rebuild_ms": rebuild_ms,
                "naive": naive_summary,
                "indexed": indexed_summary,
                "total_ratio": naive_summary["total_ms"] / indexed_summary["total_ms"],
                "mean_ratio": naive_summary["mean_ms"] / indexed_summary["mean_ms"],
            }

            await _delete_all_rules_for_pairs(pair_ids)
    finally:
        await _cleanup(pairs, user)

    return results


def format_report(results: dict) -> str:
    lines = ["# Rule index benchmark report", ""]
    lines.append(
        f"{NUM_PAIRS} synthetic pairs, real Postgres (Supabase pooler), "
        f"evaluation price {EVALUATION_PRICE} against thresholds spread 1..1,000,000."
    )
    lines.append("")
    lines.append(
        "Read the methodology docstring at the top of `scripts/benchmark_rule_index.py` "
        "before citing a number — the speedup is expected to come mostly from eliminating "
        "a network round-trip, not from algorithmic complexity, and both are reported "
        "separately below."
    )
    lines.append("")

    lines.append("## Per-poll-cycle cost (120 pairs evaluated once)")
    lines.append("")
    lines.append(
        "| rules/pair | total rules | naive total (ms) | indexed total (ms) | ratio | "
        "naive mean (ms) | indexed mean (ms) | naive p95 (ms) | indexed p95 (ms) |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for scale, data in results.items():
        n, i = data["naive"], data["indexed"]
        lines.append(
            f"| {scale} | {data['rule_count_indexed']} | {n['total_ms']:.1f} | {i['total_ms']:.2f} | "
            f"{data['total_ratio']:.0f}x | {n['mean_ms']:.3f} | {i['mean_ms']:.4f} | "
            f"{n['p95_ms']:.3f} | {i['p95_ms']:.4f} |"
        )
    lines.append("")

    lines.append("## One-time index rebuild cost (not on the hot path, but real)")
    lines.append("")
    lines.append("| rules/pair | total rules | rebuild time (ms) |")
    lines.append("| --- | --- | --- |")
    for scale, data in results.items():
        lines.append(f"| {scale} | {data['rule_count_indexed']} | {data['rebuild_ms']:.1f} |")
    lines.append("")

    worst = results[max(results.keys())]
    lines.append(
        f"**At the PRD's stated worst case ({max(results.keys())} rules/pair, "
        f"{worst['rule_count_indexed']} total rules across {NUM_PAIRS} pairs):** "
        f"one poll cycle costs {worst['naive']['total_ms']:.0f}ms with the naive per-pair "
        f"query vs {worst['indexed']['total_ms']:.2f}ms indexed — a "
        f"{worst['total_ratio']:.0f}x reduction, measured against real Postgres, not assumed."
    )
    lines.append("")

    return "\n".join(lines)


async def main() -> None:
    results = await run_benchmark()
    report = format_report(results)
    print(report)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report)
    print(f"\n(also written to {REPORT_PATH})")


if __name__ == "__main__":
    asyncio.run(main())
