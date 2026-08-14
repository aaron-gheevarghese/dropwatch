import boto3

from config.settings import settings


def get_sns_client():
    # Same auth pattern as sqs/client.py: no static credentials, boto3's default chain
    # resolves the EC2 instance's attached IAM role in deployment.
    return boto3.client("sns", region_name=settings.aws_region)


def get_topic_arn(client, topic_name: str) -> str:
    # Unlike SQS's create_queue, SNS's create_topic has no attribute-conflict pitfall —
    # it's AWS's own idempotent get-or-create, safe to call just to resolve an ARN.
    # Actual provisioning (including the email subscription) is scripts/setup_sns.py's
    # job, run separately, same split as sqs.client.get_queue_url vs scripts/setup_sqs.py.
    return client.create_topic(Name=topic_name)["TopicArn"]
