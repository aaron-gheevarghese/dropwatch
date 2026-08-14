from config.settings import settings
from workers.outbox_publisher import backoff_seconds


def test_backoff_grows_exponentially() -> None:
    values = [backoff_seconds(attempt) for attempt in range(1, 6)]
    assert values == [5, 10, 20, 40, 80]


def test_backoff_caps_at_max() -> None:
    assert backoff_seconds(20) == settings.outbox_backoff_max_seconds


def test_backoff_first_attempt_uses_base() -> None:
    assert backoff_seconds(1) == settings.outbox_backoff_base_seconds
