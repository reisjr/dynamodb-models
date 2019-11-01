#!/usr/bin/env python3

from aws_cdk import core
from cdk.ddbmodels_stack import DynamoDBModelsStack


app = core.App()
DynamoDBModelsStack(app, "cdk")

app.synth()
