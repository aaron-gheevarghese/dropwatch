"""Idempotent SNS provisioning: creates the alert topic and its email subscription if
they don't already exist.

Safe to run repeatedly — SNS's create_topic is itself idempotent by name (returns the
same TopicArn every time). The email subscription step additionally checks existing
subscriptions before calling Subscribe, since re-subscribing an endpoint that's already
pending confirmation sends ANOTHER confirmation email each time — annoying, not
idempotent in effect even though the API technically allows it.

Note: an email subscription stays in PendingConfirmation until the person at that
address clicks the confirmation link AWS sends them. This script cannot complete that
step — it's a hard requirement of SNS's email protocol, not something to route around.

Usage: python -m scripts.setup_sns
"""

import logging

from config.settings import settings
from sns.client import get_sns_client

logger = logging.getLogger(__name__)


def ensure_topic(client) -> str:
    return client.create_topic(Name=settings.sns_topic_name)["TopicArn"]


def _existing_email_subscription(client, topic_arn: str, email: str) -> dict | None:
    paginator = client.get_paginator("list_subscriptions_by_topic")
    for page in paginator.paginate(TopicArn=topic_arn):
        for subscription in page["Subscriptions"]:
            if subscription["Protocol"] == "email" and subscription["Endpoint"] == email:
                return subscription
    return None


def ensure_email_subscription(client, topic_arn: str, email: str) -> str:
    existing = _existing_email_subscription(client, topic_arn, email)
    if existing is not None:
        logger.info("email subscription already exists for %s (status: %s)", email, existing["SubscriptionArn"])
        return existing["SubscriptionArn"]

    response = client.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    logger.info("subscribed %s — confirmation email sent, must be clicked before delivery works", email)
    return response["SubscriptionArn"]


def ensure_sns(client) -> tuple[str, str]:
    """Returns (topic_arn, subscription_arn)."""
    topic_arn = ensure_topic(client)
    subscription_arn = ensure_email_subscription(client, topic_arn, settings.alert_email)
    return topic_arn, subscription_arn


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    client = get_sns_client()
    topic_arn, subscription_arn = ensure_sns(client)
    logger.info("topic ready: %s", topic_arn)
    logger.info("subscription: %s", subscription_arn)


if __name__ == "__main__":
    main()
