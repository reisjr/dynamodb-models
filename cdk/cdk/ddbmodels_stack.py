from aws_cdk import (
    core, aws_dynamodb, aws_lambda, aws_ec2, aws_ecs,
    aws_apigateway, aws_iam, aws_s3, aws_ecr, aws_ssm, aws_codebuild
)
from aws_cdk.aws_ec2 import SubnetType, Vpc
from aws_cdk.core import App, Construct, Duration

class DynamoDBModelsStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # The code that defines your stack goes here
            # The code that defines your stack goes here
        table = aws_dynamodb.Table(self, "DashboardModel",
            partition_key=aws_dynamodb.Attribute(name="Pk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="Sk", type=aws_dynamodb.AttributeType.STRING),
            #read_capacity=3,
            #write_capacity=3)
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST)
        
        core.CfnOutput(
            self, "TableName_Dashboard",
            description="Table name for Dashboard",
            value=table.table_name
        )
        