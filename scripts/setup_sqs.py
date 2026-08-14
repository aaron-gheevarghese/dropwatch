"""Idempotent SQS provisioning: creates the poll queue and its DLQ if they don't exist,
and reconciles the poll queue's visibility timeout / redrive policy if they do.

Safe to run repeatedly (e.g. from both the scheduler and poller's container entrypoint,
or a one-off deploy step) — get-or-create by name means a second run never fails or
duplicates a queue, unlike calling `create_queue` blind with attributes that might
conflict with what's already there.

Usage: python -m scripts.setup_sqs
"""

import json
import logging

from config.settings import settings
from sqs.client import get_sqs_client

logger = logging.getLogger(__name__)


def _get_or_create_queue_url(client, name: str, attributes: dict[str, str] | None = None) -> str:
    try:
        return client.get_queue_url(QueueName=name)["QueueUrl"]
    except client.exceptions.QueueDoesNotExist:
        return client.create_queue(QueueName=name, Attributes=attributes or {})["QueueUrl"]


def ensure_dlq(client) -> tuple[str, str]:
    """Returns (queue_url, queue_arn)."""
    queue_url = _get_or_create_queue_url(client, settings.sqs_poll_dlq_name)
    arn = client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return queue_url, arn


def ensure_poll_queue(client, dlq_arn: str) -> str:
    redrive_policy = json.dumps(
        {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": settings.sqs_max_receive_count}
    )
    desired_attributes = {
        "VisibilityTimeout": str(settings.sqs_visibility_timeout_seconds),
        "RedrivePolicy": redrive_policy,
    }

    queue_url = _get_or_create_queue_url(client, settings.sqs_poll_queue_name, desired_attributes)
    # create_queue only applies Attributes on first creation; reconcile explicitly so a
    # settings change (e.g. a new visibility timeout) takes effect on an existing queue too.
    client.set_queue_attributes(QueueUrl=queue_url, Attributes=desired_attributes)
    return queue_url


def ensure_queues(client) -> tuple[str, str]:
    """Returns (poll_queue_url, dlq_url)."""
    dlq_url, dlq_arn = ensure_dlq(client)
    poll_url = ensure_poll_queue(client, dlq_arn)
    return poll_url, dlq_url


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    client = get_sqs_client()
    poll_url, dlq_url = ensure_queues(client)
    logger.info("poll queue ready: %s", poll_url)
    logger.info("DLQ ready: %s", dlq_url)


if __name__ == "__main__":
    main()
