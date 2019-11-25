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

        #kda_app = aws_kinesisanalytics.CfnApplicationV2(self, "kda_agg",
        #    runtime_environment="SQL-1_0", 
        #    service_execution_role=kda_service_role.role_arn, 
        #    application_configuration=None,
        #    application_description="Sample aggregation application", 
        #    application_name="DashboardMetricsAggregator"
        #)

        # kda_record_format = aws_kinesisanalytics.RecordFormatProperty(
        #     record_format_type="CSV"
        # )

        # kda_schema = aws_kinesisanalytics.InputSchemaProperty(
        #     record_columns= [ ],
        #     #"RecordEncoding" : String,
        #     record_format=kda_record_format
        # )

            #     InputSchema:
            # RecordColumns:
            #  - Name: "example"
            #    SqlType: "VARCHAR(16)"
            #    Mapping: "$.example"
            # RecordFormat:
            #   RecordFormatType: "JSON"
            #   MappingParameters:
            #     JSONMappingParameters:
            #       RecordRowPath: "$"

        # kda_inputs = aws_kinesisanalytics.InputProperty(
        #     name_prefix="bla",
        #     kinesis_streams_input=kds_input_stream,
        #     input_schema=kda_schema,
        # )

        input = {
            "NamePrefix": "exampleNamePrefix",
            "InputSchema": {
             "RecordColumns": {
                "Name": "example",
                "SqlType": "VARCHAR(16)",
                "Mapping": "$.example"
             },
            "RecordFormat": {
              "RecordFormatType": "JSON",
              "MappingParameters": {
                "JSONMappingParameters": {
                  "RecordRowPath": "$"
                }
              }
            }
            }
        }

        #You chose CSV as the record format, but you haven't specified the Delimited MappingParameters. Please provide the Column and Row Delimiters via Delimited Mapping Parameters (

        col1 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="example",
            sql_type="VARCHAR(4)",
            mapping="$.example"
        )

        col2 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="ts",
            sql_type="VARCHAR(32)",
            mapping="$.ts"
        )

        schema = aws_kinesisanalytics.CfnApplication.InputSchemaProperty(
            record_columns=[col2, col1],
            record_encoding="UTF-8",
            record_format=aws_kinesisanalytics.CfnApplication.RecordFormatProperty(
                record_format_type="JSON",
                mapping_parameters=aws_kinesisanalytics.CfnApplication.MappingParametersProperty(
                    json_mapping_parameters=aws_kinesisanalytics.CfnApplication.JSONMappingParametersProperty(
                        record_row_path="$"
                    )
                )
            )
        )

        kda_is = aws_kinesisanalytics.CfnApplication.KinesisStreamsInputProperty(
            resource_arn=kds_input_stream.stream_arn,
            role_arn=kda_service_role.role_arn
        )

        ip = aws_kinesisanalytics.CfnApplication.InputProperty(
            name_prefix="SOURCE_SQL_STREAM",
            input_schema=schema,
            kinesis_streams_input=kda_is
        )

        #   KinesisStreamsInput:
        #     ResourceARN: !GetAtt InputKinesisStream.Arn
        #     RoleARN: !GetAtt KinesisAnalyticsRole.Arn

        kda_app = aws_kinesisanalytics.CfnApplication(self, "kda_agg",
            inputs=[ip], #kda_inputs,
            application_code="Example Application Code", 
            application_description="Aggregating data", 
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

        lambda_agg_function.add_environment("DDB_TABLE_DASHBOARD", table.table_name)

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
        