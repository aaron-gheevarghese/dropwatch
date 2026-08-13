# Concert Ticket Price-Drop & Event-Status Alert System

## Problem

Concertgoers do not have a simple way to receive a prompt alert when the available ticket-price range for a Ticketmaster event changes or when an event's sale status changes. This system tracks explicitly submitted Ticketmaster events and sends an SMS with the official Ticketmaster purchase link when a meaningful, API-reported change is detected.

## Goals

- Track Ticketmaster event URLs and store their event IDs and official purchase URLs.
- Poll 500+ tracked events per minute by batching Ticketmaster Inventory Status API requests, while staying within the configured API quota.
- Deliver an SMS within 60 seconds of detecting an API-reported price-range or event-status change.
- Use rolling minimum-price history and a z-score filter to distinguish meaningful drops from transient inventory noise.
- Prevent duplicate alerts during concurrent polling, SQS redelivery, and publish retries.
- Demonstrate queue-backed polling, batching, rate limiting, caching, idempotency, durable retries, and load-tested throughput.

## Non-Goals (v1)

- Sources other than Ticketmaster.
- Ticket purchasing or reservation; every alert links to Ticketmaster for checkout.
- Saved searches or automatic event discovery.
- Alert channels other than SMS.
- Guarantees that Ticketmaster's source price changes are visible within one minute. Ticketmaster refreshes Inventory Status price ranges at most hourly; the 60-second objective is measured from this system's observation of an API-reported change to SNS acceptance.

## User Flow

1. A user submits a Ticketmaster event URL to `POST /events`.
2. The API extracts the Ticketmaster event ID, retrieves initial metadata with Discovery API, and stores the official Ticketmaster purchase URL.
3. A scheduler enqueues the event for staggered polling.
4. Workers batch event IDs into Inventory Status API requests and write the observed minimum/maximum available-ticket prices to Postgres, then Redis.
5. A price drop or status change is evaluated using the event's history.
6. A qualifying change creates one durable notification and outbox event.
7. An outbox publisher sends one SMS with the event name, observed price, and direct Ticketmaster purchase URL.

## Provider Design and API Use

`TicketmasterProvider` is an adapter with the following implementations:

- `TicketmasterDiscoveryProvider`: used at ingestion and on a slower metadata refresh cadence for event name, official URL, sales dates, and event status.
- `TicketmasterInventoryProvider`: used by the minute-level poller. It batches event IDs and returns currently reported minimum/maximum available-ticket prices.
- `SimulatedTicketmasterProvider`: deterministic fixture-driven provider used for load, latency, failure, and price-change tests.

The poller must batch IDs rather than call Discovery API once per event. It must reserve quota, enforce the configured request-per-second limit, and defer work rather than exceed quota. Current state reads by the API are served from Redis; only successful pollers write fresh state to Postgres and Redis.

## Observable Changes

The v1 does not use an undefined "deal flag." It may alert on:

- `min_price_drop`: statistically significant reduction in `current_min_price`.
- `max_price_drop`: optional statistically significant reduction in `current_max_price`.
- `event_onsale`: event status changes to `onsale`.
- `event_cancelled`, `event_postponed`, or `event_rescheduled`.

Price-drop filtering uses `current_min_price` as the primary signal. A first observation establishes a baseline and never alerts. At least five successful observations are required before z-score evaluation. For a zero-variance history, a configurable minimum percentage decrease is required; otherwise no price-drop alert is emitted.

## Statistical Filter

- Rolling window: the last 20 successful minimum-price observations.
- Z-score cutoff: -2.0.
- Baseline: naive raw-delta alert on any lower observed minimum price.
- Acceptance metric: a version-controlled, labeled event-price fixture dataset must show a false-positive reduction of at least 73% versus that baseline. Fixtures include genuine drops, normal inventory fluctuations, data glitches, insufficient history, and zero-variance histories.

## Architecture

- **API:** FastAPI endpoints for event ingestion, current state, and price history.
- **Scheduler:** selects due events and sends staggered SQS poll messages.
- **Polling workers:** group due events into Inventory Status batches, persist observations, evaluate changes, and update cache.
- **Postgres (Supabase):** durable source of truth for events, histories, notifications, and outbox records.
- **Redis (ElastiCache):** cache for current event state and rate-limit counters.
- **SQS:** poll queue, dead-letter queue, and redelivery on worker failure.
- **Outbox publisher:** publishes pending notification events to AWS SNS, with durable retry.
- **EC2:** hosts the scheduler and worker pool.

## Data Model

### Event

`id`, `ticketmaster_event_id`, `url`, `purchase_url`, `name`, `venue`, `event_datetime`, `currency`, `current_min_price`, `current_max_price`, `event_status`, `last_checked_at`, `created_at`

### PriceHistory

`id`, `event_id`, `min_price`, `max_price`, `currency`, `event_status`, `checked_at`

### Notification

`id`, `event_id`, `type`, `detected_min_price`, `detected_max_price`, `detected_state_hash`, `idempotency_key` (unique), `status` (`pending`, `sent`, `failed`), `triggered_at`, `sent_at`

### OutboxEvent

`id`, `notification_id` (unique), `payload`, `publish_attempts`, `published_at`, `last_error`, `created_at`

The idempotency key is `sha256(f"{event_id}:{change_type}:{detected_state_hash}")`. The state hash includes all fields relevant to the alert, including currency and price range. A genuinely new observed state creates a new alert; redelivery of the same state does not.

## Failure-Mode Behavior

- Successful poll write order: one Postgres transaction creates the history record, updates event state, and creates a deduplicated notification plus outbox event when applicable. Redis is updated only after commit.
- Ticketmaster request failure: no observation is written; SQS redelivers the message according to its retry policy.
- Duplicate/concurrent poll: the unique idempotency key and transaction ensure one logical notification and one outbox event.
- SNS publish failure or process crash after commit: the pending outbox event is retried until SNS accepts it. A successful publish records `published_at` and notification `sent_at`.

## Constants

| Constant | Value | Rationale |
| --- | --- | --- |
| z-score cutoff | -2.0 | Tunable significance threshold |
| minimum history | 5 observations | Avoid unstable early statistics |
| rolling window | 20 successful polls | Bounded history used by filter |
| poll cadence | 60 seconds | System checks due events once per minute |
| SQS visibility timeout | 30 seconds | Retry headroom within observed-change SLA |
| SQS max receive count | 5 | Messages move to DLQ after repeated failure |

## API Contract

### `POST /events`

Request:

```json
{"url":"https://www.ticketmaster.com/event/<event-id>","poll_interval_seconds":60}
```

Response fields include `id`, `ticketmaster_event_id`, `url`, `purchase_url`, `poll_interval_seconds`, `current_min_price`, `current_max_price`, `currency`, `event_status`, `last_checked`, and `created_at`.

### Other endpoints

- `GET /events`: list tracked events with cached current state.
- `GET /events/{id}/history`: return observed price and status history.

## Secrets and Configuration

- Local development: gitignored `.env`.
- Deployment: AWS Secrets Manager retrieved through the EC2 IAM role.
- Required secrets: Ticketmaster API key, SNS configuration, Supabase connection string, Redis configuration, and SQS queue URLs.

## Acceptance Criteria

- End-to-end: a simulated or API-reported qualifying change creates one SMS request within 60 seconds of detection and includes the official Ticketmaster purchase URL.
- Load test: sustained 10-minute run for 500+ events/minute using the simulated provider; report throughput, queue backlog, error rate, and p50/p95/p99 detection-to-SNS-acceptance latency. p99 is under 60 seconds.
- Quota test: production scheduler batches Ticketmaster event IDs and never exceeds configured daily or per-second limits; deferred polls are observable.
- Filter test: labeled fixtures demonstrate at least 73% lower false-positive rate than naive raw-delta alerts; cutoff, insufficient-history, and zero-variance boundaries are tested.
- Idempotency test: concurrent duplicate polls, forced worker crash, and SQS redelivery generate exactly one notification/outbox event/SNS publish per detected state.

## Project Structure

```text
concertwatch/
├── api/
│   ├── main.py
│   ├── routes/events.py
│   └── schemas.py
├── providers/
│   ├── ticketmaster_discovery.py
│   ├── ticketmaster_inventory.py
│   └── simulated.py
├── workers/
│   ├── scheduler.py
│   ├── poller.py
│   ├── zscore_filter.py
│   └── outbox_publisher.py
├── db/
│   ├── models.py
│   └── client.py
├── cache/client.py
├── config/settings.py
├── tests/
│   ├── fixtures/
│   ├── test_zscore_filter.py
│   ├── test_idempotency.py
│   ├── test_poller.py
│   ├── test_quota.py
│   └── test_load.py
├── infra/
├── .env.example
├── requirements.txt
└── README.md
```
