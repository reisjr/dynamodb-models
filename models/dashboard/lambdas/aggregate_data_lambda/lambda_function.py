from __future__ import print_function

import base64
import json
import boto3
import random
import os

ddb_cli = boto3.resource('dynamodb')
table = ddb_cli.Table(os.getenv('DDB_TABLE_DASHBOARD'))

def lambda_handler(event, context):
    #print(json.dumps(event))
    
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        data = json.loads(payload)
        
        type = data["type"]
        
        if type == "BY_STATE":
            print("@@@@@@@@@@@@")
            print(data)
        else:
            print("##########")
            print(data)
        try:
            table.put_item(Item={
                "Pk": "{}#{}".format(data["region"], data["state"]),
                "Sk": data["INGEST_TIME"],
                "Region": "{}".format(data["region"]),
                "State": "{}".format(data["state"]),
                "StoreId": "{}".format(data["store-id"]),
                "kpi1": data["KPI_1_SUM"],
                "kpi2": data["KPI_2_SUM"],
                "kpi3": data["KPI_2_SUM"],
                "kpi4": data["KPI_2_SUM"],
                "TYPE": type
            })
        except Exception as e:
            print(e)

    return 'Successfully processed {} records.'.format(len(event['Records']))