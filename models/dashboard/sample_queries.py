
import boto3
import os
from boto3.dynamodb.conditions import Key
import json
import decimal
import re
import datetime
import pytz

table_name = os.getenv("TABLE_NAME")

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def query_store_per_period(kpi, region, state, store, period, limit=100):
    r = table.query(
        #Select="SPECIFIC_ATTRIBUTES",
        #AttributesToGet=[
        #    kpi
        #],
        Limit=limit,
        ConsistentRead=False,
        KeyConditionExpression=Key('Pk').eq("{}#{}#{}".format(region, state, store)) & Key('Sk').begins_with("{}".format(period)),
        ScanIndexForward=False,
    )

    items = {}
    if "Items" in r:
        items = r["Items"]
    
    return items

def query_state_per_period(kpi, region, state, period, limit=100):
    r = table.query(
        #Select="SPECIFIC_ATTRIBUTES",
        #AttributesToGet=[
        #    kpi
        #],
        Limit=limit,
        ConsistentRead=False,
        KeyConditionExpression=Key('Pk').eq("{}#{}".format(region, state)) & Key('Sk').begins_with("{}".format(period)),
        ScanIndexForward=False,
    )

    items = {}
    if "Items" in r:
        items = r["Items"]
    
    return items

def query_region_per_period(kpi, region, period, limit=100):
    r = table.query(
        #Select="SPECIFIC_ATTRIBUTES",
        #AttributesToGet=[
        #    kpi
        #],
        Limit=limit,
        ConsistentRead=False,
        KeyConditionExpression=Key('Pk').eq("{}".format(region)) & Key('Sk').begins_with("{}".format(period)),
        ScanIndexForward=False,
    )

    items = {}
    if "Items" in r:
        items = r["Items"]
    
    return items


def extract_timestamp(line):
    
    x = re.search("(.*)#(.*)#(.*)", line)
    
    return x.group(2) 


def print_kpi_region_state_store(kpi, period, region, state, store, records):
    print(">BY REGION STATE STORE")
    items = query_store_per_period(kpi, region, state, store, "{}#2019-11-27".format(period), records)

    #print(r)
    #print(json.dumps(items, cls=DecimalEncoder, indent=1))

    for item in items:
        ts_str = extract_timestamp(item["Sk"])
        ts = datetime.datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S.%f')
        timezone_sp = pytz.timezone('America/Sao_Paulo')
        ts_sp = pytz.utc.localize(ts, is_dst=None).astimezone(timezone_sp)
        v = item[kpi]
        #print(ts)
        #print(ts_sp)
        print("{} - {} - {}/{}/{} - {} - {}".format(ts_sp, period, region, state, store, kpi, v))

    print("--------")

def print_kpi_region_state(kpi, period, region, state, records):
    print(">BY REGION STATE")
    items = query_state_per_period(kpi, region, state, "{}#2019-11-27".format(period), records)

    #print(r)
    #print(json.dumps(items, cls=DecimalEncoder, indent=1))

    for item in items:
        ts_str = extract_timestamp(item["Sk"])
        ts = datetime.datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S.%f')
        timezone_sp = pytz.timezone('America/Sao_Paulo')
        ts_sp = pytz.utc.localize(ts, is_dst=None).astimezone(timezone_sp)
        v = item[kpi]
        #print(ts)
        #print(ts_sp)
        print("{} - {} - {}/{} - {} - {}".format(ts_sp, period, region, state, kpi, v))

    print("--------")

def print_kpi_region(kpi, period, region, records):
    print(">BY REGION")
    items = query_region_per_period(kpi, region, "{}#2019-11-27".format(period), records)

    #print(r)
    #print(json.dumps(items, cls=DecimalEncoder, indent=1))

    for item in items:
        ts_str = extract_timestamp(item["Sk"])
        ts = datetime.datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S.%f')
        timezone_sp = pytz.timezone('America/Sao_Paulo')
        ts_sp = pytz.utc.localize(ts, is_dst=None).astimezone(timezone_sp)
        v = item[kpi]
        #print(ts)
        #print(ts_sp)
        print("{} - {} - {} - {} - {}".format(ts_sp, period, region, kpi, v))

    print("--------")

if __name__ == "__main__":

    kpi = "kpi1"
    region = "SOUTHEAST"
    state = "RJ"
    records = 15
    period = "PT1H"
    store = 1

    print_kpi_region_state(kpi, period, region, state, records)

    period = "PT10M"

    print_kpi_region_state(kpi, period, region, state, records)

    period = "PT1H"

    print_kpi_region(kpi, period, region, records)

    period = "PT1M"

    print_kpi_region(kpi, period, region, records)

    print_kpi_region_state_store(kpi, period, region, state, store, records)
