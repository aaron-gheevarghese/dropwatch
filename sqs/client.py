import boto3

from config.settings import settings


def get_sqs_client():
    # No static credentials passed — boto3's default chain resolves the EC2 instance's
    # attached IAM role in deployment (or a local AWS CLI profile for dev/testing).
    return boto3.client("sqs", region_name=settings.aws_region)


def get_queue_url(client, queue_name: str) -> str:
    return client.get_queue_url(QueueName=queue_name)["QueueUrl"]
