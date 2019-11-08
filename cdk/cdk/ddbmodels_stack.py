from aws_cdk import (
    core, aws_dynamodb, aws_lambda,
    aws_iam, aws_s3, aws_kinesis,
    
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
        
        kds_input_stream = aws_kinesis.Stream(self, "dash_input_stream",
            shard_count=1, 
            stream_name="dashboard_input_stream")
        
        # Creating a ingest bucket for this stack
        ingest_bucket = aws_s3.Bucket(self,'test-ngest-bucket')

        #kda_agg_stream = 
        
        #Creating destination config
        S3_dest_config = aws_kinesisfirehose.CfnDeliveryStreamProps.s3_destination_configuration(
            BucketARN =ingest_bucket.bucket_arn,
            BufferingHints = aws_kinesisfirehose.CfnDeliveryStream.BufferingHintsProperty(IntervalInSeconds= 60,
            SizeInMBs= 5,
            'CompressionFormat'= "UNCOMPRESSED",
            "RoleARN" = " ")

        #Creating firehose for this stack
        firehose_test = aws_kinesisfirehose.CfnDeliveryStream(self, "testr",
            s3_destination_configuration=S3_dest_config)
        
        #kfh_history_stream =
        
        lambda_agg_function = aws_lambda.Function(self, "AggDataLambda",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            handler="lambda_function.lambda_handler",
            code=aws_lambda.Code.asset("../models/dashboard/aggregate_data_lambda"),
            timeout=Duration.minutes(5))

        lambda_agg_function.add_environment("BUCKET_NAME", "dreis-sandbox-temp")
        lambda_agg_function.add_environment("DDB_TABLE_DEVICE_CATALOG", table.table_name)
        
        core.CfnOutput(
            self, "TableName_Dashboard",
            description="Table name for Dashboard",
            value=table.table_name
        )
        