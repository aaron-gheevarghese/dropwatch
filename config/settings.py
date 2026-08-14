from decimal import Decimal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str

    kraken_api_base_url: str = "https://api.kraken.com/0/public"
    kraken_requests_per_second: float = 1.0

    discovery_activate_floor_usd: Decimal = Decimal("100000")
    discovery_deactivate_floor_usd: Decimal = Decimal("75000")

    default_poll_interval_seconds: int = 60

    # boto3 picks up credentials from its default chain (EC2 instance role in
    # deployment); no static AWS access keys are configured here.
    aws_region: str = "us-east-1"
    sqs_poll_queue_name: str = "dropwatch-poll-queue"
    sqs_poll_dlq_name: str = "dropwatch-poll-dlq"
    sqs_visibility_timeout_seconds: int = 30
    sqs_max_receive_count: int = 5

    sns_topic_name: str = "dropwatch-alerts"
    alert_email: str = "aaron123iseragon@gmail.com"

    outbox_backoff_base_seconds: int = 5
    outbox_backoff_max_seconds: int = 300

    zscore_window: int = 60
    zscore_min_observations: int = 30
    # No PRD-given number for this — it's explicitly "configurable". A 0.5% move on a
    # dead-flat window is the kind of thing worth a fallback alert on; much smaller than
    # that and it's more likely tick noise on a thin/rounding price than a real move.
    zscore_zero_variance_min_percent: Decimal = Decimal("0.5")

    # Docker-on-EC2 per the PRD's architecture decision, not ElastiCache. Defaults to
    # localhost for host-run dev workers; docker-compose.yml overrides this to the
    # "redis" service hostname for containerized services.
    redis_url: str = "redis://localhost:6379/0"
    rule_index_invalidation_channel: str = "rule_index_invalidate"
    # Pub/sub is the fast path; this is a safety net in case a worker misses a signal
    # (e.g. briefly disconnected from Redis) — not a substitute for it.
    rule_index_refresh_interval_seconds: int = 300
    # Without this, a pub/sub connection silently dropped by NAT/Docker networking
    # during an idle period is never detected — redis-py's PING health check only runs
    # if this is set AND the read path gives it a chance to (see cache/client.py and
    # workers/rule_index.py's _listen_for_invalidations).
    redis_health_check_interval_seconds: int = 30


settings = Settings()
