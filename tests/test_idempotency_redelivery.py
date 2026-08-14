"""Formalizes what Steps 2/3 proved manually: duplicate polls, a forced worker crash
plus SQS redelivery, and true concurrent evaluation must all produce exactly one
Notification + OutboxEvent per detected state — never zero (lost), never more than one
(duplicate delivery).
"""

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import boto3
import pytest
from moto import mock_aws
from sqlalchemy import select

from db.client import async_session_factory
from db.models import AlertRule, Notification, OutboxEvent, Pair, User
from providers.base import Quote
from rules.evaluator import evaluate_rules_for_pair
from scripts.setup_sqs import ensure_queues
from workers import poller


class _StubProvider:
    """Returns a fixed quote for a fixed pair — deterministic, no real Kraken calls."""

    def __init__(self, canonical_name: str, last: Decimal) -> None:
        self._canonical_name = canonical_name
        self._last = last

    async def get_quotes(self, canonical_names: list[str]) -> dict[str, Quote]:
        return {
            self._canonical_name: Quote(
                canonical_name=self._canonical_name,
                last=self._last,
                bid=self._last,
                ask=self._last,
                volume_24h=Decimal("1000"),
            )
        }


async def _make_pair_rule_user(session, *, threshold: Decimal) -> tuple[Pair, AlertRule, User]:
    user = User(contact="test@example.com")
    pair = Pair(
        kraken_pair_name=f"TEST{uuid4().hex[:10].upper()}USD",
        display_name="TEST/USD",
        base_currency="TEST",
        quote_currency="USD",
        poll_interval_seconds=60,
        is_active=True,
    )
    session.add_all([user, pair])
    await session.flush()

    rule = AlertRule(
        user_id=user.id,
        pair_id=pair.id,
        rule_type="absolute_above",
        threshold=threshold,
        is_enabled=True,
    )
    session.add(rule)
    await session.flush()
    return pair, rule, user


async def _cleanup_pair_and_user(pair_id, user_id) -> None:
    async with async_session_factory() as session:
        pair_row = await session.get(Pair, pair_id)
        if pair_row is not None:
            await session.delete(pair_row)  # cascades: rule -> notification -> outbox_event
        user_row = await session.get(User, user_id)
        if user_row is not None:
            await session.delete(user_row)
        await session.commit()


async def test_duplicate_poll_creates_exactly_one_notification(db_session) -> None:
    pair, _rule, _user = await _make_pair_rule_user(db_session, threshold=Decimal("100"))
    price = Decimal("150")
    observed_at = datetime(2026, 8, 15, 12, 0, 5, tzinfo=UTC)

    first = await evaluate_rules_for_pair(db_session, pair, price, observed_at)
    # Simulates a duplicate poll of the same underlying observation seconds later.
    second = await evaluate_rules_for_pair(db_session, pair, price, observed_at.replace(second=40))

    assert len(first) == 1
    assert len(second) == 0

    all_notifications = (
        (await db_session.execute(select(Notification).where(Notification.pair_id == pair.id))).scalars().all()
    )
    assert len(all_notifications) == 1

    outbox = (
        (await db_session.execute(select(OutboxEvent).where(OutboxEvent.notification_id == all_notifications[0].id)))
        .scalars()
        .all()
    )
    assert len(outbox) == 1


async def test_true_concurrent_evaluation_creates_exactly_one_notification() -> None:
    # Two independent sessions/connections racing to insert the same idempotency_key —
    # this is what a single rolled-back-savepoint fixture can't exercise, since both
    # sides need to actually commit concurrently for the unique-constraint race to occur.
    async with async_session_factory() as setup_session:
        pair, _rule, user = await _make_pair_rule_user(setup_session, threshold=Decimal("100"))
        await setup_session.commit()
        pair_id, user_id = pair.id, user.id

    price = Decimal("150")
    observed_at = datetime(2026, 8, 15, 12, 5, 0, tzinfo=UTC)

    async def _evaluate_and_commit() -> list[Notification]:
        async with async_session_factory() as session:
            pair_row = await session.get(Pair, pair_id)
            created = await evaluate_rules_for_pair(session, pair_row, price, observed_at)
            await session.commit()
            return created

    try:
        results = await asyncio.gather(_evaluate_and_commit(), _evaluate_and_commit())
        total_created = sum(len(r) for r in results)
        assert total_created == 1, f"expected exactly one winner of the race, got {total_created}"

        async with async_session_factory() as session:
            all_notifications = (
                (await session.execute(select(Notification).where(Notification.pair_id == pair_id))).scalars().all()
            )
            assert len(all_notifications) == 1
    finally:
        await _cleanup_pair_and_user(pair_id, user_id)


@pytest.mark.respx(base_url="https://api.kraken.com/0/public")
async def test_forced_crash_then_sqs_redelivery_creates_exactly_one_notification() -> None:
    async with async_session_factory() as setup_session:
        pair, _rule, user = await _make_pair_rule_user(setup_session, threshold=Decimal("100"))
        await setup_session.commit()
        pair_id, user_id = pair.id, user.id
        canonical_name = pair.kraken_pair_name

    provider = _StubProvider(canonical_name, Decimal("150"))

    try:
        with mock_aws():
            sqs_client = boto3.client("sqs", region_name="us-east-1")
            queue_url, _dlq_url = ensure_queues(sqs_client)

            sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"pair_id": str(pair_id)}))
            received = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
            messages = received["Messages"]

            # First processing attempt: DB work commits, but the delete never happens —
            # simulates the process crashing in the gap between commit and delete_message.
            async with async_session_factory() as session:
                with patch.object(poller, "_delete_batch", new=AsyncMock(return_value=None)):
                    await poller.process_batch(provider, session, sqs_client, queue_url, messages)

            # A received-but-undeleted message stays invisible for the full visibility
            # timeout (30s) — it doesn't just come back. Force that expiry immediately
            # rather than actually sleeping 30s, to simulate "later, it's redelivered".
            sqs_client.change_message_visibility(
                QueueUrl=queue_url, ReceiptHandle=messages[0]["ReceiptHandle"], VisibilityTimeout=0
            )
            still_visible = sqs_client.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=0
            )
            assert len(still_visible.get("Messages", [])) == 1, "message should still be redeliverable, not lost"

            # Redelivery: process it again for real, this time deleting on success.
            async with async_session_factory() as session:
                await poller.process_batch(provider, session, sqs_client, queue_url, still_visible["Messages"])

            gone = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
            assert gone.get("Messages", []) == [], "message should be deleted after the successful redelivery"

        async with async_session_factory() as session:
            notifications = (
                (await session.execute(select(Notification).where(Notification.pair_id == pair_id))).scalars().all()
            )
            assert len(notifications) == 1, "redelivery/reprocessing must not duplicate the alert"

            outbox = (
                (
                    await session.execute(
                        select(OutboxEvent).where(OutboxEvent.notification_id == notifications[0].id)
                    )
                )
                .scalars()
                .all()
            )
            assert len(outbox) == 1
    finally:
        await _cleanup_pair_and_user(pair_id, user_id)
