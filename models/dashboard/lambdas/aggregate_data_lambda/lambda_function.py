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
    
    cnt_total = 0
    cnt_ok = 0
    cnt_err = 0

    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        data = json.loads(payload)
        
        type = data["type"]
        
        cnt_total += 1

        if "EVENT_TIME" not in data:
            print("Adding EVENT_TIME...")
            data["EVENT_TIME"] = "2019-01-01"
        
        if "INGEST_TIME" not in data:
            print("Adding INGEST_TIME...")
            data["INGEST_TIME"] = "2019-01-01"

        try:
            if type == "BY_STORE_PT1M":
                table.put_item(Item={
                    "Pk": "{}#{}#{}".format(data["region"], data["state"], str(data["store-id"])),
                    "Sk": "{}#{}#{}".format("PT1M", data["EVENT_TIME"], random.randint(0,10000)),
                    "Region": "{}".format(data["region"]),
                    "State": "{}".format(data["state"]),
                    "StoreId": "{}".format(data["store-id"]),
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
                })
            elif type == "BY_STATE_PT1M":
                table.put_item(Item={
                    "Pk": "{}#{}".format(data["region"], data["state"]),
                    "Sk": "{}#{}#{}".format("PT1M", data["EVENT_TIME"], random.randint(0,10000)),
                    "Region": "{}".format(data["region"]),
                    "State": "{}".format(data["state"]),
                    "StoreId": "0",
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
                })
            elif type == "BY_REGION_PT1M":
                table.put_item(Item={
                    "Pk": "{}".format(data["region"]),
                    "Sk": "{}#{}#{}".format("PT1M", data["EVENT_TIME"], random.randint(0,10000)),
                    "Region": "{}".format(data["region"]),
                    "State": "NA",
                    "StoreId": "0",
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
                })
            elif type == "BY_STORE_PT10M":
                table.put_item(Item={
                    "Pk": "{}#{}#{}".format(data["region"], data["state"], str(data["store-id"])),
                    "Sk": "{}#{}#{}".format("PT10M", data["EVENT_TIME"], random.randint(0,10000)),
                    "Region": "{}".format(data["region"]),
                    "State": "{}".format(data["state"]),
                    "StoreId": "{}".format(data["store-id"]),
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
                })
            elif type == "BY_STATE_PT10M":
                table.put_item(Item={
                    "Pk": "{}#{}".format(data["region"], data["state"]),
                    "Sk": "{}#{}#{}".format("PT10M", data["EVENT_TIME"], random.randint(0,10000)),
                    "Region": "{}".format(data["region"]),
                    "State": "{}".format(data["state"]),
                    "StoreId": "0",
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
                })
            elif type == "BY_REGION_PT10M":
                table.put_item(Item={
                    "Pk": "{}".format(data["region"]),
                    "Sk": "{}#{}#{}".format("PT10M", data["EVENT_TIME"], random.randint(0,10000)),
                    "Region": "{}".format(data["region"]),
                    "State": "NA",
                    "StoreId": "0",
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
                })
            elif type == "BY_STORE_PT1H":
                table.put_item(Item={
                    "Pk": "{}#{}#{}".format(data["region"], data["state"], str(data["store-id"])),
                    "Sk": "{}#{}#{}".format("PT1H", data["EVENT_TIME"], random.randint(0,10000)),
                    "Region": "{}".format(data["region"]),
                    "State": "{}".format(data["state"]),
                    "StoreId": "{}".format(data["store-id"]),
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
                })
            elif type == "BY_STATE_PT1H":
                table.put_item(Item={
                    "Pk": "{}#{}".format(data["region"], data["state"]),
                    "Sk": "{}#{}#{}".format("PT1H", data["EVENT_TIME"], random.randint(0,10000)),
                    "Region": "{}".format(data["region"]),
                    "State": "{}".format(data["state"]),
                    "StoreId": "0",
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
                })
            elif type == "BY_REGION_PT1H":
                table.put_item(Item={
                    "Pk": "{}".format(data["region"]),
                    "Sk": "{}#{}#{}".format("PT1H", data["EVENT_TIME"], random.randint(0,10000)),
                    "Region": "{}".format(data["region"]),
                    "State": "NA",
                    "StoreId": "0",
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
                })
                cnt_ok += 1
            else:
                cnt_err += 1
                table.put_item(Item={
                    "Pk": "{}".format(data["region"]),
                    "Sk": "{}#{}#{}".format("UNK", data.get("EVENT_TIME", "2019"), random.randint(0,10000)),
                    "Region": "{}".format(data["region"]),
                    "State": "NA",
                    "StoreId": "0",
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
                })
        except Exception as e:
            cnt_err += 1
            print("ERROR: {}".format(e))

    print('RESPONSE TOTAL: {} OKAY: {} ERR: {}'.format(cnt_total, cnt_ok, cnt_err))
    
    return 'RESPONSE TOTAL: {} OKAY: {} ERR: {}'.format(cnt_total, cnt_ok, cnt_err)