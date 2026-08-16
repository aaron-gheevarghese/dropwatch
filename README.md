# dropwatch

Tracks a set of Kraken USD trading pairs, polls their prices on a schedule, and sends
an email alert (via SNS) when a price crosses a threshold you've configured — with
idempotent delivery (no duplicate alerts from redelivery/retries) and per-rule cooldown
suppression (no alert spam during a sustained move).

## Architecture

Five long-running processes, one Postgres database, one SQS queue pair, one SNS topic,
one Redis instance:

| Process | Does |
| --- | --- |
| `api` (FastAPI) | `POST/GET /pairs`, `POST/PATCH /rules`, `POST /rules/backtest`, `GET /alerts` |
| `workers/discovery.py` | Daily job. Scans all Kraken USD pairs, tracks/untracks by a $100k/$75k notional-volume hysteresis floor |
| `workers/scheduler.py` | Every 5s, enqueues one staggered SQS message per pair that's due for a poll |
| `workers/poller.py` | SQS consumer. Batches due pairs into one Kraken `Ticker` call, writes `PriceHistory`, evaluates alert rules against an in-memory rule index, commits — deletes the SQS message only on success |
| `workers/outbox_publisher.py` | Polls pending `OutboxEvent` rows and publishes them to SNS, with exponential backoff on failure |

Data flow: `discovery` decides what's tracked → `scheduler` decides when to poll it →
`poller` does the poll, writes history, and evaluates rules → a qualifying rule writes
a `Notification` + `OutboxEvent` in the *same transaction* as the price observation →
`outbox_publisher` is the only thing that talks to SNS, decoupled from evaluation so a
firing is never lost even if SNS is briefly unreachable.

Rule evaluation (`rules/evaluator.py`) supports all five rule types: `absolute_below`/
`absolute_above` (fixed threshold), `zscore_move` (statistical: fires when a price's log
return is an outlier relative to that pair's own recent rolling volatility, not a fixed
percentage — see `workers/zscore_filter.py` and the measurement note below),
`percent_change` (price moved more than `percent`% over the trailing `window_seconds`,
compared against the most recent real observation at or before that instant — never
interpolated), and `spread_widen` (`(ask - bid) / bid` exceeds `percent`%). Rules come
from `workers/rule_index.py`, an in-memory index bucketed by pair and pre-sorted where a
short-circuit is possible — see [Rule index](#rule-index) below for why and by how much.

```
Kraken API
    │
    ▼
discovery.py ──► pairs (Postgres)
    │
    ▼
scheduler.py ──► SQS poll queue ──► poller.py ──► price_history (Postgres)
                       │                │              │
                       ▼                │              ▼
                   SQS DLQ  ◄───────────┘   rule_index.py (in-memory) ◄── Redis pub/sub
                (after 5 failed                        │                  (POST/PATCH /rules
                 receives)                              ▼                  invalidates)
                                          notifications + outbox_events
                                                         │
                                                         ▼
                                          outbox_publisher.py ──► SNS ──► email
```

## Prerequisites

- Python 3.12+
- A Supabase project (Postgres, accessed through its transaction-mode connection pooler)
- AWS credentials with SQS + SNS permissions — either a local AWS CLI profile (dev) or
  an EC2 instance role (deployment). No static keys are read from `.env`; boto3's
  default credential chain handles this.
- Redis, reachable at `REDIS_URL` — run via `docker compose up redis` or any local
  install. Docker-on-EC2 in deployment, not ElastiCache (a deliberate architecture
  choice, not a cost shortcut waiting to be fixed).

## Setup

1. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure `.env`** (see [Environment variables](#environment-variables) below).
   The only two things you must fill in yourself are `DATABASE_URL` (from your Supabase
   project's dashboard → Project Settings → Database → Connection string → URI tab,
   Transaction mode, port 6543 — copy it verbatim, then prefix the driver with
   `postgresql+asyncpg://`) and `ALERT_EMAIL`.

3. **Run migrations:**

   ```bash
   python -m alembic upgrade head
   ```

4. **Provision AWS resources** (idempotent — safe to re-run):

   ```bash
   python -m scripts.setup_sqs   # poll queue + DLQ, redrive policy
   python -m scripts.setup_sns   # alert topic + email subscription
   ```

   The email subscription stays in `PendingConfirmation` until whoever owns
   `ALERT_EMAIL` clicks the confirmation link AWS sends them — delivery won't work
   until that happens.

5. **Seed the (single, v1) user:**

   ```bash
   python -m scripts.seed_user
   ```

   Logs the seeded user's UUID — you need it as `user_id` when creating rules via
   `POST /rules`. There's currently no endpoint to look this up later; check the
   `users` table or re-run this script (it's idempotent and will just report the
   existing row).

## Running locally

Each process is a separate `python -m` entrypoint — run whichever ones you need in
separate terminals:

```bash
uvicorn api.main:app --reload
python -m workers.scheduler
python -m workers.poller
python -m workers.outbox_publisher
python -m workers.discovery  # one-shot; intended to run on a daily schedule
```

## Running via Docker Compose

```bash
docker compose up --build
```

Brings up `redis`, `scheduler`, `poller`, and `outbox_publisher` — `poller` is the only
service that actually depends on `redis` (rule index invalidation); the others don't
need it and don't wait on it. Run the setup scripts (migrations, `setup_sqs`,
`setup_sns`, `seed_user`) once beforehand — they're deliberately not part of the
compose stack, since they're one-off provisioning steps, not long-running processes.
The API isn't in `docker-compose.yml` yet either; run it locally or add a service for
it the same way (it'll need `REDIS_URL` too, to publish rule-change invalidations).

## Environment variables

| Variable | Required | Default | What it's for |
| --- | --- | --- | --- |
| `DATABASE_URL` | yes | — | Supabase pooler URI, `postgresql+asyncpg://` scheme |
| `KRAKEN_API_BASE_URL` | no | `https://api.kraken.com/0/public` | |
| `KRAKEN_REQUESTS_PER_SECOND` | no | `1.0` | Rate limit for Kraken `Ticker`/`AssetPairs` calls |
| `DISCOVERY_ACTIVATE_FLOOR_USD` | no | `100000` | 24h notional volume to start tracking a pair |
| `DISCOVERY_DEACTIVATE_FLOOR_USD` | no | `75000` | 24h notional volume to stop tracking a pair (hysteresis band between this and the floor above) |
| `DEFAULT_POLL_INTERVAL_SECONDS` | no | `60` | Default `Pair.poll_interval_seconds` |
| `AWS_REGION` | no | `us-east-1` | Region for SQS + SNS |
| `SQS_POLL_QUEUE_NAME` | no | `dropwatch-poll-queue` | |
| `SQS_POLL_DLQ_NAME` | no | `dropwatch-poll-dlq` | |
| `SQS_VISIBILITY_TIMEOUT_SECONDS` | no | `30` | |
| `SQS_MAX_RECEIVE_COUNT` | no | `5` | Receives before a message moves to the DLQ |
| `SNS_TOPIC_NAME` | no | `dropwatch-alerts` | |
| `ALERT_EMAIL` | yes (for delivery) | — | Where alert emails go — also seeded as the v1 user's `contact` |
| `OUTBOX_BACKOFF_BASE_SECONDS` | no | `5` | Outbox publisher retry backoff, first attempt |
| `OUTBOX_BACKOFF_MAX_SECONDS` | no | `300` | Outbox publisher retry backoff cap |
| `ZSCORE_WINDOW` | no | `60` | Rolling baseline size (prior returns) for `zscore_move` |
| `ZSCORE_MIN_OBSERVATIONS` | no | `30` | Minimum prior returns required before a `zscore_move` rule can fire at all |
| `ZSCORE_ZERO_VARIANCE_MIN_PERCENT` | no | `0.5` | Fallback minimum percent move required to fire when the baseline window has zero variance (division by zero otherwise) |
| `REDIS_URL` | no | `redis://localhost:6379/0` | Rule index invalidation pub/sub. `docker-compose.yml` overrides this to `redis://redis:6379/0` for the containerized poller |
| `RULE_INDEX_INVALIDATION_CHANNEL` | no | `rule_index_invalidate` | Pub/sub channel `POST`/`PATCH /rules` publish to and the poller subscribes to |
| `RULE_INDEX_REFRESH_INTERVAL_SECONDS` | no | `300` | Periodic safety-net rebuild in case a worker misses a pub/sub signal (e.g. briefly disconnected from Redis) — not a substitute for it |
| `REDIS_HEALTH_CHECK_INTERVAL_SECONDS` | no | `30` | How often redis-py PINGs an idle pub/sub connection to detect a silently-dead one (see the post-Step-6 fix note under [Rule index](#rule-index)) |

## API

- `POST /pairs`, `GET /pairs` — track/list Kraken pairs
- `POST /rules` — create an alert rule, any of the five implemented types
- `PATCH /rules/{id}` — enable, disable, or modify an existing rule
- `POST /rules/backtest` — replay an unsaved rule definition against real `PriceHistory`
  for one pair. See [Backtest](#backtest) below.
- `GET /alerts` — alert history: rule fired, observed price, delivery status (including
  suppression reason), outbox retry diagnostics. Paginated (`limit`/`offset`), filterable
  by `pair_id`, `rule_id`, `status`.

## Backtest

`POST /rules/backtest` takes the same rule-definition fields as `POST /rules` (minus
`user_id` — nothing is attributed or saved — and `is_enabled`, which isn't meaningful
for a hypothetical rule) plus `lookback_days`, and replays it against real
`PriceHistory` for one pair. Returns fire count, fires/day, per-fire timestamps and
prices, and a post-cooldown count (what would actually have been delivered vs. what
matched the condition).

It reuses `evaluate_rules_for_pair` — the exact function the live poller calls — row by
row over history, in its own session that's opened and never committed. That's the
whole point: results can't drift from what the live engine would actually have done,
because it's not a separate implementation approximating that behavior, it's the same
code. The unsaved rule is genuinely `session.add()`/`flush()`'d (not just held in
memory) so it satisfies `notifications.rule_id`'s real foreign key during replay, the
same as any other integrity constraint the live path relies on — never committing is
what makes "no persistence" true, not skipping the insert.

Two bugs surfaced testing this against real data across all 118 tracked pairs, both
now covered by `tests/test_backtest.py`:
- A temp rule that's *never* added to the session fails that foreign key on every
  fire, silently — caught by the same `IntegrityError` handler built for
  `evaluate_rules_for_pair`'s concurrent-race case — producing a false `fire_count=0`
  regardless of the rule. (This is why the rule now gets flushed for real, above.)
- `Notification.triggered_at` is `server_default=func.now()`, and Postgres's `now()`
  is the *transaction* timestamp — constant across every statement in one bulk replay
  transaction. Reading it back per-fire reported the same wall-clock instant for every
  fire regardless of which historical moment actually triggered it. Fixed by tracking
  each fire's real `observed_at` in the replay loop instead of trusting that column.

Performance note, also found by testing against real data: replay cost scales with
*fire count*, not row count — 2000+ rows with zero fires replay in a few seconds, since
non-firing rows only cost an in-memory index lookup; a rule matching a large fraction
of observations (which isn't a realistic alert threshold anyway) is slow, because each
fire pays the same real per-fire cost the live poller does (a `SAVEPOINT`, an
idempotency check, an insert) — over a remote pooled connection, at high fire-density,
that adds up. Not optimized away, since doing so would mean the backtest no longer
reuses the live path unchanged.

## Statistical filter (zscore_move)

`workers/zscore_filter.py` computes `z = (r_t - mean(r_window)) / stdev(r_window)` over
log returns from real `PriceHistory` rows, with a 60-observation rolling window, a
30-observation minimum before it'll fire at all, and a configurable minimum-percent
fallback for zero-variance windows. No interpolation — a gap between two real
observations just becomes one return computed over however much time actually elapsed.

**Measured, not asserted** — `python -m scripts.build_zscore_fixture` extracts real
captured price history into the version-controlled `tests/fixtures/zscore_fixture.json`
(118 real pairs, ~142k observations), and `python -m scripts.measure_zscore_filter`
measures the filter against it and writes `docs/zscore_measurement_report.md`. The
finding is more nuanced than a single percentage — read the full report, but in short:
z-score's fire rate is **~5.5x more consistent across pairs of different volatility**
than a fixed threshold (that's the actual mechanism this feature is supposed to
provide, and it measures out real), while the literal "total fires vs. one fixed
threshold" framing is threshold-dependent and often *unfavorable* to z-score at
realistic thresholds, because a 2-sigma cutoff has a fixed ~4.55%+ theoretical fire
rate that a fixed percentage threshold isn't bound by. No specific percentage from
that report should be quoted out of context.

## Rule index

`workers/rule_index.py` keeps AlertRules in memory, bucketed by pair, instead of the
poller running one DB query per pair per poll cycle. Within a bucket, `absolute_below`
is sorted descending by threshold and `absolute_above` ascending, so evaluation stops
at the first non-matching rule instead of scanning the rest — see the module's
docstring for why the sort order guarantees that's safe. `zscore_move` has no such
ordering (firing depends on sigma/direction against one shared per-pair z-score, and
checking that is O(1) regardless of order) so it's just a small unsorted list.

Freshness: built from Postgres on worker start (blocking — the poller won't process
messages against an empty index), kept fresh via Redis pub/sub (`POST`/`PATCH /rules`
publish, the poller subscribes and rebuilds on signal), plus a periodic fallback
rebuild as a safety net if a signal is ever missed. A rebuild is atomic from a reader's
perspective — built fully off to the side, then swapped in with one reference
assignment.

**Post-Step-6 fix:** pub/sub invalidation worked in every test (including against a
real local Redis) but silently stopped working on the deployed EC2 instance after some
idle time. Root cause: the listener used `pubsub.listen()`, which does one *unbounded*
blocking read per message — and redis-py's connection health-check PING only gets a
chance to run at the start of a read call, so a single unbounded read blocks that check
out entirely. If the underlying TCP connection is silently dropped (NAT/Docker
networking dropping an idle connection with no FIN/RST — common in cloud networking,
and never reproducible against `fakeredis`, which has no real TCP layer to go stale
on), `listen()` just hangs forever: no exception, no log line, nothing. Fixed by (1)
enabling `health_check_interval` on the client (`cache/client.py`, disabled by default)
and (2) switching the listener to `pubsub.get_message(timeout=...)` polling
(`workers/rule_index.py`), which returns on its own on a schedule and gives the health
check real opportunities to fire and detect a truly dead connection. Verified against a
real local Redis server: confirmed periodic health-check `PING`s actually go out on the
wire while idle, and confirmed the listener detects a killed/restarted Redis process
and reconnects. Also added: logging around both the publish (`api/routes/rules.py` —
notably, `publish()`'s return value tells you how many subscribers were listening at
that instant, which is 0 if no worker was connected) and the subscribe/receive loop, so
this class of issue is observable next time instead of silent.

**Measured, not asserted** — `python -m scripts.benchmark_rule_index` seeds ~120
synthetic pairs and up to 400 rules/pair (the PRD's own example scale) into real
Postgres, times the naive per-pair query against the live Supabase pooler, times the
indexed lookup+scan, and writes `docs/rule_index_benchmark_report.md`. Measured result
at the worst-case scale: **~228x faster per poll cycle** (5175ms naive vs 22.7ms
indexed for 120 pairs), growing to ~3330x at a lighter, still-realistic 10 rules/pair.
As expected and stated up front in the report's methodology, this is dominated by
eliminating a network round-trip per pair, not by algorithmic cleverness — Postgres
itself scans even 400 rows in sub-millisecond CPU time; the point of the index is never
making that round-trip at all.

## Testing

```bash
python -m pytest
```

Most tests are fully mocked (Kraken via `respx`, AWS via `moto`, Redis via `fakeredis`
— an in-memory server, same role as `moto` but for Redis) and need no live services. A
subset — idempotency/redelivery races and cooldown suppression — run against the real
(dev) Supabase database, wrapped in a transaction that's rolled back after each test
(see `tests/conftest.py`); nothing they do is ever actually committed. This needs a
working `DATABASE_URL` in `.env` to run.

## Build status

- **Step 1 — Core loop:** Kraken provider, `Pair`/`PriceHistory` models, discovery job,
  poller (originally a plain loop), `POST/GET /pairs`. Done.
- **Step 2 — SQS:** scheduler/poller split, staggered enqueue, DLQ redrive. Done.
- **Step 3 — Rules and delivery:** `AlertRule`/`Notification`/`OutboxEvent`/`User`
  models, rule evaluation wired into the poller, `POST /rules`, SNS provisioning,
  outbox publisher. Done.
- **Step 4 — Hardening:** cooldown suppression, `GET /alerts`, `PATCH /rules/{id}`,
  formalized idempotency/redelivery/cooldown test suite. Done.
- **Step 5 — Statistical filter:** `zscore_move` rule type, real-data measurement
  against a version-controlled fixture. Done — see
  [Statistical filter](#statistical-filter-zscore_move) above. The measured result is a
  genuine finding, not a target hit: z-score gives ~5.5x more consistent alert rates
  across pairs of different volatility than a fixed threshold, which is the real,
  substantiated value proposition; the literal "% fewer false positives than one fixed
  threshold" framing turned out to be threshold-dependent and often unfavorable at
  realistic thresholds. Full writeup in `docs/zscore_measurement_report.md`.
- **Step 6 — Rule index and benchmark:** in-memory rule index bucketed by pair,
  sorted for short-circuit evaluation; Redis (Docker-on-EC2, not ElastiCache) added for
  pub/sub invalidation with a periodic safety-net rebuild; poller wired to the index
  instead of a per-pair DB query. Done — see [Rule index](#rule-index) above. Measured
  ~228x per-poll-cycle speedup at the PRD's 400-rules/pair worst case, real Postgres
  timings, not assumed. Full writeup in `docs/rule_index_benchmark_report.md`.
- **Step 7 — Backtest endpoint, remaining rule types:** `percent_change` and
  `spread_widen` rule types, wired into `rules/evaluator.py` and
  `workers/rule_index.py` alongside the other three. `POST /rules/backtest`, reusing
  `evaluate_rules_for_pair` unchanged rather than a parallel implementation. Done — see
  [Backtest](#backtest) above, including two real bugs (a silent false-negative and a
  wrong-timestamp bug) found and fixed by testing against real accumulated data across
  all 118 tracked pairs rather than only synthetic fixtures.
- **Not yet built:**
  - EC2 deploy (currently runs from a laptop/dev machine only)
  - Load testing at the 500+ pairs/minute target
  - A way to discover the seeded user's ID via the API (currently: check the DB or
    the seed script's log output)
  - The API service isn't in `docker-compose.yml` (only the four background workers are)
