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


settings = Settings()
