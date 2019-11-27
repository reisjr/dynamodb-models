#!/usr/bin/env python3

from aws_cdk import core
from cdk.cdk_stack import DynamoDBModelsStack
from cdk.cdk_test_stack import DynamoDBModelsTestStack

app = core.App()
DynamoDBModelsStack(app, "cdk")
DynamoDBModelsTestStack(app, "test")

app.synth()
