from aws_cdk import (
    core, aws_dynamodb, aws_lambda,
    aws_iam, aws_s3, aws_kinesis,
    aws_kinesisfirehose,
    aws_kinesisanalytics,
    aws_lambda_event_sources
)
from aws_cdk.aws_ec2 import SubnetType, Vpc
from aws_cdk.core import App, Construct, Duration
#from aws_cdk.aws_kinesisfirehose import IntervalInSeconds

class DynamoDBModelsStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # The code that defines your stack goes here
        table = aws_dynamodb.Table(self, "DashboardModel",
            partition_key=aws_dynamodb.Attribute(name="Pk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="Sk", type=aws_dynamodb.AttributeType.STRING),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST)
        
        kds_input_stream = aws_kinesis.Stream(self, "kds_dashboard_input_stream",
            shard_count=1, 
            stream_name="kds_dashboard_input_stream")
        
        # Creating a ingest bucket for this stack
        ingest_bucket = aws_s3.Bucket(self,'dreis-dboard-ingest-bucket')

        kfh_service_role = aws_iam.Role(self, 'KFH_Dashboard_Role',
            assumed_by=aws_iam.ServicePrincipal('firehose.amazonaws.com')
        )

        kfh_policy_stmt = aws_iam.PolicyStatement(
            actions=["*"],
            resources=["*"]
        )

        kfh_service_role.add_to_policy(kfh_policy_stmt)

        #Creating firehose for this stack
        kfh_datalake = aws_kinesisfirehose.CfnDeliveryStream(self, "kfh_datalake",
            s3_destination_configuration= aws_kinesisfirehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=ingest_bucket.bucket_arn,
                buffering_hints=aws_kinesisfirehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60,
                    size_in_m_bs=5),
                compression_format="UNCOMPRESSED",
                role_arn=kfh_service_role.role_arn
                )
        )

        kda_service_role = aws_iam.Role(self, 'KDA_Dashboard_Role',
            assumed_by=aws_iam.ServicePrincipal('kinesisanalytics.amazonaws.com')
        )

        kda_policy_stmt = aws_iam.PolicyStatement(
            actions=["*"],
            resources=["*"]
        )

        kda_service_role.add_to_policy(kda_policy_stmt)

        kda_app = aws_kinesisanalytics.CfnApplicationV2(self, "kda_agg",
            runtime_environment="SQL-1_0", 
            service_execution_role=kda_service_role.role_arn, 
            application_configuration=None,
            application_description="Sample aggregation application", 
            application_name="DashboardMetricsAggregator"
        )

        kds_output_stream = aws_kinesis.Stream(self, "kds_dashboard_output_stream",
            shard_count=1, 
            stream_name="kds_dashboard_output_stream")
        
        lambda_agg_function = aws_lambda.Function(self, "AggDataLambda",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            handler="lambda_function.lambda_handler",
            code=aws_lambda.Code.asset("../models/dashboard/lambdas/aggregate_data_lambda"),
            timeout=Duration.minutes(5))

        lambda_agg_function.add_environment("BUCKET_NAME", "dreis-sandbox-temp")
        lambda_agg_function.add_environment("DDB_TABLE_DEVICE_CATALOG", table.table_name)

        lambda_agg_function.add_to_role_policy(aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            actions=[
                "kinesis:*"
            ],
            resources=["*"]
        ))

        table.grant_read_write_data(lambda_agg_function)

        kes = aws_lambda_event_sources.KinesisEventSource(kds_output_stream,
            starting_position=aws_lambda.StartingPosition.TRIM_HORIZON,
            batch_size=100, 
            #max_batching_window=100
        )

        lambda_agg_function.add_event_source(kes)

        core.CfnOutput(
            self, "TableName_Dashboard",
            description="Table name for Dashboard",
            value=table.table_name
        )
        