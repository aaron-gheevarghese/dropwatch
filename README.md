# dropwatch

Tracks a set of Kraken USD trading pairs, polls their prices on a schedule, and sends
an email alert (via SNS) when a price crosses a threshold you've configured — with
idempotent delivery (no duplicate alerts from redelivery/retries) and per-rule cooldown
suppression (no alert spam during a sustained move).

## Architecture

Five long-running processes, one Postgres database, one SQS queue pair, one SNS topic:

| Process | Does |
| --- | --- |
| `api` (FastAPI) | `POST/GET /pairs`, `POST/PATCH /rules`, `GET /alerts` |
| `workers/discovery.py` | Daily job. Scans all Kraken USD pairs, tracks/untracks by a $100k/$75k notional-volume hysteresis floor |
| `workers/scheduler.py` | Every 5s, enqueues one staggered SQS message per pair that's due for a poll |
| `workers/poller.py` | SQS consumer. Batches due pairs into one Kraken `Ticker` call, writes `PriceHistory`, evaluates alert rules, commits — deletes the SQS message only on success |
| `workers/outbox_publisher.py` | Polls pending `OutboxEvent` rows and publishes them to SNS, with exponential backoff on failure |

Data flow: `discovery` decides what's tracked → `scheduler` decides when to poll it →
`poller` does the poll, writes history, and evaluates rules → a qualifying rule writes
a `Notification` + `OutboxEvent` in the *same transaction* as the price observation →
`outbox_publisher` is the only thing that talks to SNS, decoupled from evaluation so a
firing is never lost even if SNS is briefly unreachable.

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
                   SQS DLQ  ◄───────────┘        rules/evaluator.py
                (after 5 failed                        │
                 receives)                              ▼
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

Brings up `scheduler`, `poller`, and `outbox_publisher` as three services from one
image. Run the setup scripts (migrations, `setup_sqs`, `setup_sns`, `seed_user`) once
beforehand — they're deliberately not part of the compose stack, since they're one-off
provisioning steps, not long-running processes. The API isn't in `docker-compose.yml`
yet either; run it locally or add a service for it the same way.

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

## API

- `POST /pairs`, `GET /pairs` — track/list Kraken pairs
- `POST /rules` — create an alert rule (`absolute_below`/`absolute_above` only for now)
- `PATCH /rules/{id}` — enable, disable, or modify an existing rule
- `GET /alerts` — alert history: rule fired, observed price, delivery status (including
  suppression reason), outbox retry diagnostics. Paginated (`limit`/`offset`), filterable
  by `pair_id`, `rule_id`, `status`.

## Testing

```bash
python -m pytest
```

Most tests are fully mocked (Kraken via `respx`, AWS via `moto`) and need no live
services. A subset — idempotency/redelivery races and cooldown suppression — run
against the real (dev) Supabase database, wrapped in a transaction that's rolled back
after each test (see `tests/conftest.py`); nothing they do is ever actually committed.
This needs a working `DATABASE_URL` in `.env` to run.

## Build status

- **Step 1 — Core loop:** Kraken provider, `Pair`/`PriceHistory` models, discovery job,
  poller (originally a plain loop), `POST/GET /pairs`. Done.
- **Step 2 — SQS:** scheduler/poller split, staggered enqueue, DLQ redrive. Done.
- **Step 3 — Rules and delivery:** `AlertRule`/`Notification`/`OutboxEvent`/`User`
  models, rule evaluation wired into the poller, `POST /rules`, SNS provisioning,
  outbox publisher. Done.
- **Step 4 — Hardening:** cooldown suppression, `GET /alerts`, `PATCH /rules/{id}`,
  formalized idempotency/redelivery/cooldown test suite. Done (this step).
- **Not yet built:**
  - EC2 deploy (currently runs from a laptop/dev machine only)
  - `percent_change`, `zscore_move`, `spread_widen` rule types (Step 7)
  - Indexed/batched rule evaluation — currently a naive per-pair scan (Step 6)
  - Load testing at the 500+ pairs/minute target
  - A way to discover the seeded user's ID via the API (currently: check the DB or
    the seed script's log output)
