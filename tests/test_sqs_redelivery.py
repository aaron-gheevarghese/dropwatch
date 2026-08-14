"""Verifies the redelivery path Step 2 adds: a poller that fails mid-poll and never
calls delete_message must not lose the message. It should become visible again after
the visibility timeout, and after enough failed attempts it should land in the DLQ
rather than disappearing. Uses moto's in-memory SQS — no real AWS calls.
"""

import json
import time
from uuid import uuid4

import boto3
import pytest
from moto import mock_aws

from scripts.setup_sqs import ensure_queues


@pytest.fixture
def sqs_client():
    with mock_aws():
        yield boto3.client("sqs", region_name="us-east-1")


def test_ensure_queues_creates_poll_queue_and_dlq_with_redrive_policy(sqs_client, monkeypatch) -> None:
    poll_url, dlq_url = ensure_queues(sqs_client)

    dlq_arn = sqs_client.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    poll_attrs = sqs_client.get_queue_attributes(
        QueueUrl=poll_url, AttributeNames=["VisibilityTimeout", "RedrivePolicy"]
    )["Attributes"]

    assert poll_attrs["VisibilityTimeout"] == "30"
    redrive = json.loads(poll_attrs["RedrivePolicy"])
    assert redrive["deadLetterTargetArn"] == dlq_arn
    assert redrive["maxReceiveCount"] == 5


def test_ensure_queues_is_idempotent(sqs_client) -> None:
    first_poll_url, first_dlq_url = ensure_queues(sqs_client)
    second_poll_url, second_dlq_url = ensure_queues(sqs_client)

    assert first_poll_url == second_poll_url
    assert first_dlq_url == second_dlq_url


def test_forced_failure_leaves_message_for_redelivery_not_lost(sqs_client) -> None:
    poll_url, _dlq_url = ensure_queues(sqs_client)
    # Shrink the visibility timeout just for this test so it doesn't take 30s to observe.
    sqs_client.set_queue_attributes(QueueUrl=poll_url, Attributes={"VisibilityTimeout": "1"})

    pair_id = str(uuid4())
    sqs_client.send_message(QueueUrl=poll_url, MessageBody=json.dumps({"pair_id": pair_id}))

    # Simulates the poller: receive, then a forced failure/crash before delete_message.
    first = sqs_client.receive_message(
        QueueUrl=poll_url, MaxNumberOfMessages=1, WaitTimeSeconds=0, AttributeNames=["ApproximateReceiveCount"]
    )
    received = first.get("Messages", [])
    assert len(received) == 1
    assert json.loads(received[0]["Body"])["pair_id"] == pair_id
    assert received[0]["Attributes"]["ApproximateReceiveCount"] == "1"
    # No delete_message call here — this is the failure being simulated.

    # Still within the visibility timeout: the message must not be handed out again.
    immediately_after = sqs_client.receive_message(QueueUrl=poll_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert immediately_after.get("Messages", []) == []

    time.sleep(1.5)

    # Past the visibility timeout: redelivered, not lost.
    redelivered = sqs_client.receive_message(
        QueueUrl=poll_url, MaxNumberOfMessages=1, WaitTimeSeconds=0, AttributeNames=["ApproximateReceiveCount"]
    )
    messages = redelivered.get("Messages", [])
    assert len(messages) == 1
    assert json.loads(messages[0]["Body"])["pair_id"] == pair_id
    assert messages[0]["Attributes"]["ApproximateReceiveCount"] == "2"


def test_message_moves_to_dlq_after_max_receive_count(sqs_client) -> None:
    poll_url, dlq_url = ensure_queues(sqs_client)
    # No wait needed to observe the redrive itself — set visibility timeout to 0 so a
    # failed (non-deleted) message is immediately eligible for redelivery again.
    sqs_client.set_queue_attributes(QueueUrl=poll_url, Attributes={"VisibilityTimeout": "0"})

    pair_id = str(uuid4())
    sqs_client.send_message(QueueUrl=poll_url, MessageBody=json.dumps({"pair_id": pair_id}))

    # Fail (receive without deleting) sqs_max_receive_count (5) times.
    for _ in range(5):
        response = sqs_client.receive_message(QueueUrl=poll_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
        assert len(response.get("Messages", [])) == 1

    # One more failed attempt: the redrive policy should have moved it to the DLQ by now.
    final_poll_check = sqs_client.receive_message(QueueUrl=poll_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert final_poll_check.get("Messages", []) == []

    dlq_check = sqs_client.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    dlq_messages = dlq_check.get("Messages", [])
    assert len(dlq_messages) == 1
    assert json.loads(dlq_messages[0]["Body"])["pair_id"] == pair_id
