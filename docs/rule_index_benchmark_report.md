# Rule index benchmark report

120 synthetic pairs, real Postgres (Supabase pooler), evaluation price 500000 against thresholds spread 1..1,000,000.

Read the methodology docstring at the top of `scripts/benchmark_rule_index.py` before citing a number — the speedup is expected to come mostly from eliminating a network round-trip, not from algorithmic complexity, and both are reported separately below.

## Per-poll-cycle cost (120 pairs evaluated once)

| rules/pair | total rules | naive total (ms) | indexed total (ms) | ratio | naive mean (ms) | indexed mean (ms) | naive p95 (ms) | indexed p95 (ms) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10 | 1200 | 4456.8 | 1.34 | 3330x | 37.140 | 0.0112 | 40.110 | 0.0210 |
| 100 | 12000 | 4714.9 | 5.14 | 917x | 39.291 | 0.0429 | 40.777 | 0.0455 |
| 400 | 48000 | 5175.4 | 22.74 | 228x | 43.128 | 0.1895 | 58.982 | 0.1977 |

## One-time index rebuild cost (not on the hot path, but real)

| rules/pair | total rules | rebuild time (ms) |
| --- | --- | --- |
| 10 | 1200 | 343.1 |
| 100 | 12000 | 549.6 |
| 400 | 48000 | 1586.9 |

**At the PRD's stated worst case (400 rules/pair, 48000 total rules across 120 pairs):** one poll cycle costs 5175ms with the naive per-pair query vs 22.74ms indexed — a 228x reduction, measured against real Postgres, not assumed.
