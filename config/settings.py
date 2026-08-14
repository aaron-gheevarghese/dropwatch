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


settings = Settings()
