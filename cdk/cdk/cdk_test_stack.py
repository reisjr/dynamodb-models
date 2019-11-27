from aws_cdk import (
    core, aws_dynamodb, aws_lambda,
    aws_iam, aws_s3, aws_kinesis,
    aws_kinesisfirehose,
    aws_kinesisanalytics,
    aws_lambda_event_sources
)
from aws_cdk.aws_ec2 import SubnetType, Vpc
from aws_cdk.core import App, Construct, Duration
import json


class DynamoDBModelsTestStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # TODO: Test
        