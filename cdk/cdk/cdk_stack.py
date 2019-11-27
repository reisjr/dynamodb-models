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
        
        kds_output_stream = aws_kinesis.Stream(self, "kds_dashboard_output_stream",
            shard_count=1, 
            stream_name="kds_dashboard_output_stream")

        # Creating a ingest bucket for this stack
        ingest_bucket = aws_s3.Bucket(self,'dreis_dboard_ingest_bucket')

        kfh_service_role = aws_iam.Role(self, 'KFH_Dashboard_Role',
            assumed_by=aws_iam.ServicePrincipal('firehose.amazonaws.com')
        )

        kfh_policy_stmt = aws_iam.PolicyStatement(
            actions=["*"],
            resources=["*"]
        )

        kfh_service_role.add_to_policy(kfh_policy_stmt)

        #Creating firehose for this stack
        kfh_source = aws_kinesisfirehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
            kinesis_stream_arn=kds_input_stream.stream_arn,
            role_arn=kfh_service_role.role_arn
        )

        kfh_datalake = aws_kinesisfirehose.CfnDeliveryStream(self, "kfh_datalake",
            s3_destination_configuration=aws_kinesisfirehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=ingest_bucket.bucket_arn,
                buffering_hints=aws_kinesisfirehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60,
                    size_in_m_bs=5),
                compression_format="UNCOMPRESSED",
                role_arn=kfh_service_role.role_arn
                ),
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=kfh_source
        )

        kda_service_role = aws_iam.Role(self, 'KDA_Dashboard_Role',
            assumed_by=aws_iam.ServicePrincipal('kinesisanalytics.amazonaws.com')
        )

        kda_policy_stmt = aws_iam.PolicyStatement(
            actions=["*"],
            resources=["*"]
        )

        kda_service_role.add_to_policy(kda_policy_stmt)

        # KA doesn't like - (dash) in names
        col1 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="state",
            sql_type="VARCHAR(2)",
            mapping="$.state"
        )

        col2 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="event_time",
            sql_type="TIMESTAMP",
            mapping="$.event-time"
        )
        
        col3 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="region",  
            sql_type="VARCHAR(12)",
            mapping="$.region"
        )

        col4 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="store_id",
            sql_type="INTEGER",
            mapping="$.store-id"
        )

        col5 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="kpi_1",
            sql_type="INTEGER",
            mapping="$.kpi-1"
        )
        
        col6 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="kpi_2",
            sql_type="INTEGER",
            mapping="$.kpi-2"
        )

        col7 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="kpi_3",
            sql_type="INTEGER",
            mapping="$.kpi-3"
        )

        col8 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="kpi_4",
            sql_type="INTEGER",
            mapping="$.kpi-4"
        )

        col9 = aws_kinesisanalytics.CfnApplication.RecordColumnProperty(
            name="kpi_5",
            sql_type="INTEGER",
            mapping="$.kpi-5"
        )

        schema = aws_kinesisanalytics.CfnApplication.InputSchemaProperty(
            record_columns=[col2, col1, col3, col4, col5, col6, col7, col8, col9],
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

        application_code = "CREATE OR REPLACE STREAM \"DESTINATION_SQL_STREAM_BY_STORE\" (\"region\" VARCHAR(10), \"state\" VARCHAR(2), \"store-id\" INTEGER, kpi_1_sum INTEGER,  kpi_2_sum INTEGER, ingest_time TIMESTAMP);" + \
            "CREATE OR REPLACE STREAM \"DESTINATION_SQL_STREAM_BY_STATE\" (\"region\" VARCHAR(10), \"state\" VARCHAR(2), kpi_1_sum INTEGER,  kpi_2_sum INTEGER, ingest_time TIMESTAMP);" + \
            "CREATE OR REPLACE STREAM \"DESTINATION_SQL_STREAM_BY_REGION\" (\"region\" VARCHAR(10), kpi_1_sum INTEGER,  kpi_2_sum INTEGER, ingest_time TIMESTAMP);" + \
            "CREATE OR REPLACE PUMP \"STREAM_PUMP\" AS INSERT INTO \"DESTINATION_SQL_STREAM_BY_STORE\"" + \
            "SELECT STREAM \"region\", \"state\", \"store-id\", SUM(\"kpi-1\") AS kpi_1_sum, SUM(\"kpi-2\") AS kpi_2_sum, FLOOR(\"SOURCE_SQL_STREAM_001\".APPROXIMATE_ARRIVAL_TIME TO MINUTE) as ingest_time" + \
            "FROM \"SOURCE_SQL_STREAM_001\"" + \
            "GROUP BY \"region\", \"state\", \"store-id\", FLOOR(\"SOURCE_SQL_STREAM_001\".APPROXIMATE_ARRIVAL_TIME TO MINUTE), FLOOR((\"SOURCE_SQL_STREAM_001\".ROWTIME - TIMESTAMP '1970-01-01 00:00:00') SECOND / 10 TO SECOND);" + \
            "CREATE OR REPLACE PUMP \"STREAM_PUMP\" AS INSERT INTO \"DESTINATION_SQL_STREAM_BY_STATE\"" + \
            "SELECT STREAM \"region\", \"state\", SUM(\"kpi-1\") AS kpi_1_sum, SUM(\"kpi-2\") AS kpi_2_sum, FLOOR(\"SOURCE_SQL_STREAM_001\".APPROXIMATE_ARRIVAL_TIME TO MINUTE) as ingest_time" + \
            "FROM \"SOURCE_SQL_STREAM_001\"" + \
            "GROUP BY \"region\", \"state\", FLOOR(\"SOURCE_SQL_STREAM_001\".APPROXIMATE_ARRIVAL_TIME TO MINUTE), FLOOR((\"SOURCE_SQL_STREAM_001\".ROWTIME - TIMESTAMP '1970-01-01 00:00:00') SECOND / 10 TO SECOND);" + \
            "CREATE OR REPLACE PUMP \"STREAM_PUMP\" AS INSERT INTO \"DESTINATION_SQL_STREAM_BY_REGION\"" + \
            "SELECT STREAM \"region\", SUM(\"kpi-1\") AS kpi_1_sum, SUM(\"kpi-2\") AS kpi_2_sum, FLOOR(\"SOURCE_SQL_STREAM_001\".APPROXIMATE_ARRIVAL_TIME TO MINUTE) as ingest_time" + \
            "FROM \"SOURCE_SQL_STREAM_001\"" + \
            "GROUP BY \"region\", FLOOR(\"SOURCE_SQL_STREAM_001\".APPROXIMATE_ARRIVAL_TIME TO MINUTE), FLOOR((\"SOURCE_SQL_STREAM_001\".ROWTIME - TIMESTAMP '1970-01-01 00:00:00') SECOND / 10 TO SECOND);"

        kda_app = aws_kinesisanalytics.CfnApplication(self, "kda_agg",
            inputs=[ip], #kda_inputs,
            application_code=application_code, 
            application_description="Aggregating data", 
            application_name="DashboardMetricsAggregator"
        )

        kda_output_prop = aws_kinesisanalytics.CfnApplicationOutput.KinesisStreamsOutputProperty(
            resource_arn=kds_output_stream.stream_arn,
            role_arn=kda_service_role.role_arn
        )

        kda_dest_schema = aws_kinesisanalytics.CfnApplicationOutput.DestinationSchemaProperty(
            record_format_type="JSON"
        )

        kda_output_prop_by_store = aws_kinesisanalytics.CfnApplicationOutput.OutputProperty(
            destination_schema=kda_dest_schema,
            kinesis_streams_output=kda_output_prop,
            name="DESTINATION_SQL_STREAM_BY_STORE"
        )

        kda_output_prop_by_state = aws_kinesisanalytics.CfnApplicationOutput.OutputProperty(
            destination_schema=kda_dest_schema,
            kinesis_streams_output=kda_output_prop,
            name="DESTINATION_SQL_STREAM_BY_STATE"
        )

        kda_output_prop_by_region = aws_kinesisanalytics.CfnApplicationOutput.OutputProperty(
            destination_schema=kda_dest_schema,
            kinesis_streams_output=kda_output_prop,
            name="DESTINATION_SQL_STREAM_BY_REGION"
        )

        kda_app_output_prop = aws_kinesisanalytics.CfnApplicationOutput(self, "kda_agg_output_store",
            application_name="DashboardMetricsAggregator",
            output=kda_output_prop_by_store
        )

        kda_app_output_prop = aws_kinesisanalytics.CfnApplicationOutput(self, "kda_agg_output_state",
            application_name="DashboardMetricsAggregator",
            output=kda_output_prop_by_state
        )

        kda_app_output_prop = aws_kinesisanalytics.CfnApplicationOutput(self, "kda_agg_output_region",
            application_name="DashboardMetricsAggregator",
            output=kda_output_prop_by_region
        )

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
            batch_size=50, 
            #max_batching_window=100
        )

        lambda_agg_function.add_event_source(kes)

        core.CfnOutput(
            self, "TableName_Dashboard",
            description="Table name for Dashboard",
            value=table.table_name
        )

        core.CfnOutput(
            self, "BucketName_Dashboard",
            description="Bucket name",
            value=ingest_bucket.bucket_arn
        )

        core.CfnOutput(
            self, "KinesisInputStream_Dashboard",
            description="Kinesis input for Dashboard",
            value=kds_input_stream.stream_name
        )

        core.CfnOutput(
            self, "KinesisOutputStream_Dashboard",
            description="Kinesis output for Dashboard",
            value=kds_output_stream.stream_name
        )
        