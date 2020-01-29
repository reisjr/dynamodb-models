import boto3
import logging
import os
import random
import json
import datetime
import time
import decimal
from string import ascii_letters
import uuid
from concurrent.futures import ThreadPoolExecutor

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)
TABLE_NAME = os.getenv('TABLE_NAME', 'DEFAULT_NAME')
PAGE_SIZE = 100

# https://en.wikipedia.org/wiki/ISO_8601
SPAN_PT10M = "PT10M"
SPAN_PT1M = "PT1M"
SPAN_PT1H = "PT1H"

REGIONS = {
    "NORTH": ["AM", "RR", "PA", "TO", "AC", "RO", "MA", "AP"],
    "NORTHEAST": ["MA", "BA", "AL", "PE", "RN", "CE", "PB", "SE", "PI"],
    "SOUTH": ["RS", "PR", "SC"],
    "SOUTHEAST": ["MG", "SP", "RJ", "ES"],
    "MIDWEST": [ "MT", "GO", "MS"]
}

letters = ascii_letters[:12]

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class DashboardModel:

    _ddb_cli = None
    _ddb_res = None
    _table = None


    def __init__(self, table_name='DEFAULT_NAME'):

        self._table_name = table_name

        # Create item in DynamoDB
        self._ddb_cli = boto3.client('dynamodb')
        self._ddb_res = boto3.resource('dynamodb')
        self._table = self._ddb_res.Table(table_name)


    def query_region_by_period(self, region, period=SPAN_PT1M, newer_than=None):
        records = {}
        records_parsed = 0
        records_returned = 0 

        q = self._table.query(
            KeyConditionExpression="Pk = :pk AND begins_with(Sk, :sk)",
            ExpressionAttributeValues={
                ":pk": "{}".format(region),
                ":sk": "{}#".format(period)
            },
            ConsistentRead=False,
            ScanIndexForward=True,
            Limit=PAGE_SIZE,
            ReturnConsumedCapacity="TOTAL"
        )
        
        print(q["ConsumedCapacity"])
        print(q)
        finished = False
        records_parsed += len(q["Items"])

        for item in q["Items"]:
            ingest_time = datetime.datetime.strptime(item["IngestTime"], '%Y-%m-%d %H:%M:%S.%f')
            if ingest_time >= newer_than:
                records[item["IngestTime"]] = item                
                records_returned += 1
            else:
                finished = True
        
        while not finished:
            q = self._table.query(
                KeyConditionExpression="Pk = :pk AND begins_with(Sk, :sk)",
                ExpressionAttributeValues={
                    ":pk": "{}".format(region),
                    ":sk": "{}#".format(period)
                },
                ConsistentRead=False,
                ScanIndexForward=True,
                ExclusiveStartKey=q["LastEvaluatedKey"],
                Limit=PAGE_SIZE,
                ReturnConsumedCapacity="TOTAL"
            )
            
            print("Consumed: {} LEN: {}".format(q["ConsumedCapacity"], len(q["Items"])))
            
            records_parsed += len(q["Items"])

            for item in q["Items"]:
                ingest_time = datetime.datetime.strptime(item["IngestTime"], '%Y-%m-%d %H:%M:%S.%f')
                if ingest_time >= newer_than:
                    records[item["IngestTime"]] = item                
                    records_returned += 1
                else:
                    finished = True
                if "LastEvaluatedKey" not in q:
                    finished = True
            else:
                finished = True

        print("RECORDS: {} / {}".format(records_parsed, len(records)))

        return records


    def query_company_by_minute_range(self, kpi_1, init, end):
        q = self._table.query(
            KeyConditionExpression="Pk = :pk AND Sk BETWEEN :init AND :end",
            ExpressionAttributeValues={
                ":pk": "{}".format(kpi_1),
                ":init": "SLOT#{}".format(init),
                ":end": "SLOT#{}".format(end)
            },
            ConsistentRead=False,
            ScanIndexForward=True,
            ReturnConsumedCapacity="TOTAL"
        )

        print(q["ConsumedCapacity"])

        return q

    def aggregate_by_day(self, records):
        agg = {}

        for k in records.keys():
            ingest_time = datetime.datetime.strptime(k, '%Y-%m-%d %H:%M:%S.%f')
            day = ingest_time.strftime("%Y-%m-%d")
            v = {}
            v = agg.get(day, {})
            
            for kpi in records.get(k).keys():
                #print("KPI {} K: {}".format(kpi, records.get(k)))
                if kpi.startswith("kpi"):
                    nv = v.get(kpi, 0) + records.get(k)[kpi]
                    v[kpi] = nv
            
            agg[day] = v

        return agg


    def query_company_by_day(self, kpi_1):
        q = self._table.query(
            KeyConditionExpression="Pk = :pk AND begins_with(Sk, :sk)",
            ExpressionAttributeValues={
                ":pk": "{}".format(kpi_1),
                ":sk": "D#"
            },
            ConsistentRead=False,
            ScanIndexForward=True,
            ReturnConsumedCapacity="TOTAL"
        )

        print(q["ConsumedCapacity"])

        return q

    def query_company_by_month(self, kpi_1):
        q = self._table.query(
            KeyConditionExpression="Pk = :pk AND begins_with(Sk, :sk)",
            ExpressionAttributeValues={
                ":pk": "{}".format(kpi_1),
                ":sk": "M#"
            },
            ConsistentRead=False,
            ScanIndexForward=True,
            ReturnConsumedCapacity="TOTAL"
        )

        print(q["ConsumedCapacity"])

        return q

    def insert_new_month_record(self, data):
        logging.debug(">insert_new_month_record")

        self._table.put_item(Item={
            "Pk": data["kpi_1"],
            "Sk": "{}#{}".format(SPAN_PT10M, data["date"]),
            "kpi_1": data["kpi_2"],
            "kpi_2": data["kpi_3"],
            "TicketMedio": data["kpi_4"],
            "FatBruto": data["kpi_5"],
            "kpi_6": data["kpi_6"],
            "kpi_7": data["kpi_7"],
            "QtdAtendimentos": data["kpi_8"],
            "ValorTotal": data["kpi_9"]
        })


    def insert_new_minute_record(self, data):
        logging.debug(">insert_new_minute_record")

        self._table.put_item(Item={
            "Pk": data["kpi_1"],
            "Sk": "m#{}".format(data["date"]),
            "kpi_1": data["kpi_2"],
            "kpi_2": data["kpi_3"],
            "TicketMedio": data["kpi_4"],
            "FatBruto": data["kpi_5"],
            "kpi_6": data["kpi_6"],
            "kpi_7": data["kpi_7"],
            "QtdAtendimentos": data["kpi_8"],
            "ValorTotal": data["kpi_9"]
        })

    def insert_record(self, record):
        logging.debug(">insert_record")

        self._table.put_item(Item=record)

#####

def random_timestamp(start, end):
    frmt = '%Y-%m-%d %H:%M:%S'

    stime = time.mktime(time.strptime(start, frmt))
    etime = time.mktime(time.strptime(end, frmt))

    ptime = stime + random.random() * (etime - stime)
    dt = datetime.datetime.fromtimestamp(time.mktime(time.localtime(ptime)))
    
    return dt


def random_date():
    start_dt = datetime.date.today().replace(day=1, month=1).toordinal()
    end_dt = datetime.date.today().toordinal()
    random_day = datetime.date.fromordinal(random.randint(start_dt, end_dt))

    return random_day


def random_times():
    return random_timestamp("2015-01-01 13:30:00", "2019-11-01 04:50:34")


def random_month_formatted():
    return random_date().strftime("%Y-%m")


def random_date_formatted():
    return random_date().strftime("%Y-%m-%d")


def random_hour_minute_formatted():
    return random_times().strftime("%Y-%m-%d_%H:%M")


def generate_random_data(period="D"):
    data = {}

    if period == "D":
        data["date"] = random_date_formatted()
        data["Sk"] = "{}#{}".format(SPAN_PT1M, data["date"])
    elif period == "M":
        data["date"] = random_month_formatted()
        data["Sk"] = "{}#{}".format(SPAN_PT10M, data["date"])
    else:
        data["date"] = random_hour_minute_formatted()
        data["Sk"] = "{}#{}".format(SPAN_PT1M, data["date"])

    data["Pk"] = random.choice(list(REGIONS.keys()))
    data["kpi_1"] = random.randint(0, 1000)
    data["kpi_2"] = random.randint(0, 1000)
    data["kpi_3"] = random.randint(0, 100)
    data["kpi_4"] = random.randint(1, 50)
    data["kpi_5"] = random.randint(1, 1000)
    data["kpi_6"] = random.randint(0, 1000)
    data["kpi_7"] = random.randint(0, 1000)
    data["kpi_8"] = random.randint(0, 1000)
    data["kpi_9"] = random.randint(0, 1000)

    return data

def load_days(n):
    logging.info(">load_data {}".format(n))

    dm = DashboardModel(table_name=TABLE_NAME)
    
    for i in range(1, 100):
        data = generate_random_data()
        dm.insert_record(data)
        #companies.add(data["kpi_1"])
        
        if i % 50 == 0:
            logging.info("THREAD {} I {}".format(n, i))


def load_hour_min(n):
    logging.info(">load_data {}".format(n))

    dm = DashboardModel(table_name=TABLE_NAME)
    
    for i in range(1, 100):
        data = generate_random_data(period="mm")
        dm.insert_new_minute_record(data)
        #companies.add(data["kpi_1"])
        
        if i % 50 == 0:
            logging.info("THREAD {} I {}".format(n, i))


def test_query():
    for region in REGIONS:
        start = time.time()
        #q = dm.query_company_by_minute(cpy)
        q = dm.query_region_by_period(region,period=SPAN_PT1M, newer_than=None)
        
        kpi_4 = 0
        kpi_2 = 0
        kpi_3 = 0
        kpi_5 = 0
        recs = 0

        for items in q['Items']:
            #print(items)
            kpi_4 += items["kpi_4"]
            kpi_2 += items["kpi_1"]
            kpi_3 += items["kpi_2"]
            kpi_5 += items["kpi_5"]
            recs += 1
            #"kpi_6": data["kpi_6"],
            #"kpi_7": data["kpi_7"],
            #"QtdAtendimentos": data["kpi_8"],
            #"ValorTotal": data["kpi_9"]

        #print(q)
        end = time.time()
        print(end - start)
        print("\n####\nCOMPANY {}\nkpi_4 {} FAT_LIQ {} kpi_3 {}\nRECS {}".format(cpy, kpi_4, kpi_2, kpi_3, recs))



if __name__ == "__main__":
    dm = DashboardModel(table_name=TABLE_NAME)
    
    logging.info(random_hour_minute_formatted())
    logging.info("{}".format(random.choice([REGIONS.keys()])))

    rec = generate_random_data("D")
    logging.info(rec)
    dm.insert_record(rec)

    region = random.choice(list(REGIONS.keys()))
    print("REGION: {}".format(region))

    newer_than = datetime.datetime.strptime("2019-11-01 02:00:00.000", '%Y-%m-%d %H:%M:%S.%f')

    q = dm.query_region_by_period(region, period=SPAN_PT1M, newer_than=newer_than)
    agg = dm.aggregate_by_day(q)
    print(agg)
    
    #print(q)

    # with ThreadPoolExecutor(max_workers=8) as executor:
    #     future = executor.submit(load_days, (1))
    #     future = executor.submit(load_days, (2))
    #     future = executor.submit(load_days, (3))
    #     future = executor.submit(load_days, (4))
    #     future = executor.submit(load_days, (5))
    #     future = executor.submit(load_days, (6))
    #     future = executor.submit(load_days, (7))
    #     future = executor.submit(load_days, (8))


    # with ThreadPoolExecutor(max_workers=8) as executor:
    #     future = executor.submit(load_hour_min, (1))
    #     future = executor.submit(load_hour_min, (2))
    #     future = executor.submit(load_hour_min, (3))
    #     future = executor.submit(load_hour_min, (4))
    #     future = executor.submit(load_hour_min, (5))
    #     future = executor.submit(load_hour_min, (6))
    #     future = executor.submit(load_hour_min, (7))
    #     future = executor.submit(load_hour_min, (8))
    
    #test_query()