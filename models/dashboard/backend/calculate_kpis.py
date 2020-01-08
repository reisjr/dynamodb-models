# -*- coding: utf-8 -*-
import dash
import dash_html_components as html
import dash_core_components as dcc
import numpy as np
from plotly import graph_objs as go
from dash.dependencies import Input, Output, State
import time
import boto3
import os
import json
import decimal
import random
import pprint

TABLE_NAME = os.getenv("TABLE_NAME", "cdk-DashboardModel0F8D7DBF-3NKS2U5YZ10Z")

ddb_res = boto3.resource("dynamodb")
table = ddb_res.Table(TABLE_NAME)

pp = pprint.PrettyPrinter(indent=4)

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


app = dash.Dash(__name__)

REGIONS = {
    "NORTH": ["AM", "RR", "PA", "TO", "AC", "RO", "MA", "AP"],
    "NORTHEAST": ["MA", "BA", "AL", "PE", "RN", "CE", "PB", "SE", "PI"],
    "SOUTH": ["RS", "PR", "SC"],
    "SOUTHEAST": ["MG", "SP", "RJ", "ES"],
    "MIDWEST": [ "MT", "GO", "MS"]
}


def query_data(region, state, time="PT10M", gt=""):
    print("region = {} / state = {} / time = {} / gt = {}".format(region, state, time, gt))

    pk = region

    if state:
        pk = "{}#{}".format(region, state)

    q = table.query(
        KeyConditionExpression="Pk = :pk AND begins_with(Sk, :ts)",
        ExpressionAttributeValues={
            ":pk": "{}".format(pk),
            ":ts": "{}#{}".format(time, gt)
        },
        Limit=50,
        ConsistentRead=False,
        ScanIndexForward=False,
        ReturnConsumedCapacity="TOTAL"
    )

    print(q["ConsumedCapacity"])
    #pp.pprint(q)

    records = {}

    if "Items" in q:
        cnt = 0
        for item in q["Items"]:
            kpis = {}
            
            kpis.setdefault("kpi1", item["kpi1"])
            kpis.setdefault("kpi2", item["kpi2"])
            kpis.setdefault("kpi3", item["kpi3"])
            kpis.setdefault("kpi4", item["kpi4"])

            records.setdefault(item.get("IngestTime", ""), kpis)
            
            #if cnt % 100 == 0:
            #    print(item)
            cnt += 1
    else:
        print("No data found!")

    while "LastEvaluatedKey" in q:
        last_eval_key = q.get("LastEvaluatedKey")

        print("Last eval {}".format(q.get("LastEvaluatedKey")))

        q = table.query(
            KeyConditionExpression="Pk = :pk AND begins_with(Sk, :ts)",
            ExpressionAttributeValues={
                ":pk": "{}".format(pk),
                ":ts": "{}#{}".format(time, gt)
            },
            ExclusiveStartKey=last_eval_key,
            Limit=100,
            ConsistentRead=False,
            ScanIndexForward=False,
            ReturnConsumedCapacity="TOTAL"
        )
        print(q["ConsumedCapacity"])

        if "Items" in q:
            for item in q["Items"]:
                kpis = {}
                
                kpis.setdefault("kpi1", item["kpi1"])
                kpis.setdefault("kpi2", item["kpi2"])
                kpis.setdefault("kpi3", item["kpi3"])
                kpis.setdefault("kpi4", item["kpi4"])

                records.setdefault(item.get("IngestTime", ""), kpis)
                
                #if cnt % 100 == 0:
                #    print(item)
                cnt += 1

    print("# Records {}".format(cnt))

    return records

def compute_kpis(rec_day, rec_month):
    kpis = {}
    k1 = 0
    k2 = 0
    k3 = 0
    k4 = 0

    for rec in rec_day.keys():
        k1 += rec_day.get(rec).get("kpi1")
        k2 += rec_day.get(rec).get("kpi2")
        k3 += rec_day.get(rec).get("kpi3")
        k4 += rec_day.get(rec).get("kpi4")

    for rec in rec_month.keys():
        k1 += rec_month.get(rec).get("kpi1")
        k2 += rec_month.get(rec).get("kpi2")
        k3 += rec_month.get(rec).get("kpi3")
        k4 += rec_month.get(rec).get("kpi4")

    kpis.setdefault("kpi1", k1)
    kpis.setdefault("kpi2", k2)
    kpis.setdefault("kpi3", k3)
    kpis.setdefault("kpi4", k4)
    
    return kpis

if __name__ == "__main__":
    region = "NORTH"
    state = "AM"

    #query today by minutes
    rec_day = query_data(region, state, time="PT10M", gt="2019-11-27")
    #pp.pprint(rec_day)

    #query remaining days by hour
    rec_month = query_data(region, state, time="PT1H", gt="")
    #pp.pprint(rec_month)

    kpis = compute_kpis(rec_day, rec_month)

    pp.pprint(kpis)

    #rec = query_data(region, state, time="PT1H", gt="2019-10")
    #pp.pprint(rec)

    #rec = query_data(region, state, time="PT1H", gt="2019-11")
    #pp.pprint(rec)

