import json
from datetime import datetime
import time
import boto3

my_stream_name = "kds_dashboard_input_stream"

kin_client = boto3.client("kinesis")

r = kin_client.describe_stream(StreamName=my_stream_name)

my_shard_id = r['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = kin_client.get_shard_iterator(
    StreamName=my_stream_name,
    ShardId=my_shard_id,
    ShardIteratorType='LATEST'
)

my_shard_iterator = shard_iterator['ShardIterator']

record_response = kin_client.get_records(ShardIterator=my_shard_iterator,
                                              Limit=2)

while 'NextShardIterator' in record_response:
    record_response = kin_client.get_records(
        ShardIterator=record_response['NextShardIterator'],
        Limit=2)

    print(record_response['Records'])

    # wait for 5 seconds
    time.sleep(5)