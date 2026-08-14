import redis.asyncio as redis

from config.settings import settings


def get_redis_client() -> redis.Redis:
    # health_check_interval matters specifically for long-lived pub/sub connections
    # (workers/rule_index.py's listener): without it, a connection silently dropped by
    # NAT/Docker networking during an idle period is never detected — redis-py only
    # sends its health-check PING from inside the read call path, so it needs a bounded
    # read (see PubSub.get_message) to ever get the chance to run. See
    # workers/rule_index.py's _listen_for_invalidations for the other half of this.
    return redis.from_url(
        settings.redis_url,
        decode_responses=True,
        health_check_interval=settings.redis_health_check_interval_seconds,
    )
