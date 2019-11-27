from __future__ import print_function

import base64
import json
import boto3
import random
import os
from collections import Counter

ddb_cli = boto3.resource('dynamodb')
table = ddb_cli.Table(os.getenv('DDB_TABLE_DASHBOARD'))


def get_period(type):
    t = type.upper()
    period = "UNK" # DEFAULT
    
    if t.find("PT1M") >= 0:
        period = "PT1M"
    if t.find("PT10M") >= 0:
        period = "PT10M"
    elif t.find("PT1H") >= 0:
        period = "PT1H"
    
    return period


def lambda_handler(event, context):
    #print(json.dumps(event))
    
    cnt_total = 0
    cnt_ok = 0
    cnt_err = 0
    stats = {
        "STORE": 0,
        "REGION": 0,
        "STATE": 0,
        "PERIODS": [],
        "STATES": [],
        "REGIONS": []
    }

    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        data = json.loads(payload)
        
        type = data.get("type", "").upper()

        #print("TYPE: '{}'".format(type))
        
        cnt_total += 1

        if "EVENT_TIME" not in data:
            print("Adding EVENT_TIME...")
            data["EVENT_TIME"] = "2019-01-01"
        
        if "INGEST_TIME" not in data:
            print("Adding INGEST_TIME...")
            data["INGEST_TIME"] = "2019-01-01"

        try:
            period = get_period(type)

            item = {
                    "Sk": "{}#{}#{}".format(period, data["EVENT_TIME"], random.randint(0,10)),
                    "Region": "{}".format(data["region"]),
                    "State": "{}".format(data.get("state", "NN")),
                    "StoreId": "{}".format(data.get("store-id", "0")),
                    "kpi1": data["KPI_1_SUM"], "kpi2": data["KPI_2_SUM"],
                    "kpi3": data["KPI_3_SUM"], "kpi4": data["KPI_4_SUM"],
                    "kpi5": data["KPI_5_SUM"],
                    "count": data.get("N_REC", 0),
                    "TYPE": type,
                    "IngestTime": data["INGEST_TIME"]
            }

            if type.find("STORE") >= 0:
                item["Pk"] = "{}#{}#{}".format(data["region"], data["state"], str(data["store-id"]))
                r = table.put_item(Item=item)
                cnt_ok += 1
                stats["STORE"] += 1
            elif type.find("STATE") >= 0:
                item["Pk"] = "{}#{}".format(data["region"], data["state"])
                r = table.put_item(Item=item)
                cnt_ok += 1
                stats["STATE"] += 1
                stats["STATES"].append(data["state"])
            elif type.find("REGION") >= 0: 
                item["Pk"] = "{}".format(data["region"]) # TBD: SHOULD ADD VIRTUAL PARTITIONS?
                r = table.put_item(Item=item)
                cnt_ok += 1
                stats["REGION"] += 1
                stats["REGIONS"].append(data["region"])
            else:
                cnt_err += 1
                item["Pk"] = "{}".format(data["region"])
                item["Sk"] = "{}#{}#{}".format("UNK", data.get("EVENT_TIME", "2019"), random.randint(0,100))
                table.put_item(Item=item)
            
            stats["PERIODS"].append(period) 
            
        except Exception as e:
            cnt_err += 1
            print("ERROR: {}".format(e))

    print('RESPONSE TOTAL: {} OKAY: {} ERR: {}'.format(cnt_total, cnt_ok, cnt_err))
    print('SUMMARY: REGION: {} STATE: {} STORE: {}'.format(stats["REGION"], stats["STATE"], stats["STORE"]))
    print('SUMMARY: PERIODS: {} STATES: {} REGIONS: {}'.format(Counter(stats["PERIODS"]), Counter(stats["STATES"]), Counter(stats["REGIONS"])))

    return 'RESPONSE TOTAL: {} OKAY: {} ERR: {}'.format(cnt_total, cnt_ok, cnt_err)