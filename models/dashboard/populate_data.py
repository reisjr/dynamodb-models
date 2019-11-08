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

COMPANY = ["hotmail.com", "gmail.com", "aol.com", "mail.com" , "mail.kz", "yahoo.com"]
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


    def deposit_money(self, acc_id, amount):
        print(">deposit_money ACC: '{}' AMOUNT: '{}'".format(acc_id, amount))

        if int(amount) <= 0:
            print("Invalid amount")
            return

        now = datetime.datetime.now().isoformat()
        tx_id = str(uuid.uuid4())
        type = "MoneyDeposit"

        r = self._ddb_cli.transact_write_items(
            TransactItems=[
                {
                    'Put': {
                        'TableName': TABLE_NAME,
                        'Item': {
                            "id": { "S": acc_id },
                            "sort": { "S" : "TX_{}".format(now)},
                            "Amount": { "N": str(amount) },
                            "SrcAccountId": { "S": "External" },
                            "DstAccountId": { "S": acc_id },
                            "CreditDebitIndicator": { "S": "Credit"},
                            "TransactionId":  { "S": tx_id },
                            "BookingDateTime": { "S": now },
                            "Type": { "S": type }
                        },
                        'ReturnValuesOnConditionCheckFailure': 'NONE'
                    }
                },
                {
                    'Update': {
                        'TableName': TABLE_NAME,
                        'Key': { 'id': { "S": acc_id }, 'sort': { "S": "BALANCE"} },
                        #'ConditionExpression': '#amount <> :tx_amount',
                        'UpdateExpression': 'SET #amount = #amount + :tx_amount',
                        'ExpressionAttributeNames': {
                            '#amount': 'Amount'
                        },
                        'ExpressionAttributeValues': {
                            ':tx_amount': { "N": str(amount) }
                        },
                        'ReturnValuesOnConditionCheckFailure': 'NONE'
                    }
                }
            ],
            ReturnConsumedCapacity="TOTAL",
            ReturnItemCollectionMetrics="SIZE"
        )

        print("####\n{}".format(json.dumps(r, indent=2, cls=DecimalEncoder)))
        return

    def withdraw_money(self, acc_id, amount):
        print(">withdraw_money ACC: {} AMOUNT: {}".format(acc_id, amount))

        if int(amount) <= 0:
            print("Invalid amount")
            return

        now = datetime.datetime.now().isoformat()
        tx_id = str(uuid.uuid4())
        type = "MoneyWithdraw"

        r = self._ddb_cli.transact_write_items(
            TransactItems=[
                {
                    'Put': {
                        'TableName': TABLE_NAME,
                        'Item': {
                            "id": { "S": acc_id },
                            "sort": { "S" : "TX_{}".format(now)},
                            "Amount": { "N": str(amount) },
                            "SrcAccountId": { "S": acc_id },
                            "DstAccountId": { "S": "External" },
                            "CreditDebitIndicator": { "S": "Debit"},
                            "TransactionId":  { "S": tx_id },
                            "BookingDateTime": { "S": now },
                            "Type": { "S": type }
                        },
                        'ReturnValuesOnConditionCheckFailure': 'NONE'
                    }
                },
                {
                    'Update': {
                        'TableName': TABLE_NAME,
                        'Key': { 'id': { "S": acc_id }, 'sort': { "S": "BALANCE"} },
                        'ConditionExpression': '#amount >= :tx_amount',
                        'UpdateExpression': 'SET #amount = #amount - :tx_amount',
                        'ExpressionAttributeNames': {
                            '#amount': 'Amount'
                        },
                        'ExpressionAttributeValues': {
                            ':tx_amount': { "N": str(amount) }
                        },
                        'ReturnValuesOnConditionCheckFailure': 'NONE'
                    }
                }
            ],
            ReturnConsumedCapacity="TOTAL",
            ReturnItemCollectionMetrics="SIZE"
        )

        print("####\n{}".format(json.dumps(r, indent=2, cls=DecimalEncoder)))
        return

    '''
    https://openbankinguk.github.io/read-write-api-site3/v3.1.3/resources-and-data-models/aisp/Transactions.html#get-transactions
    Number of decimal cases. I don't know how dynamodb handles that.
    '''
    def transfer_money(self, src_acc_id, dst_acc_id, tx_amount, info="No info"):
        print(">transfer_money FROM: {} TO: {} AMOUNT: {}".format(src_acc_id, dst_acc_id, tx_amount))

        now = datetime.datetime.now().isoformat()
        tx_id = str(uuid.uuid4())
        type = "MoneyTransfer"

        tx_resp = {
            "Amount": tx_amount,
            "SrcAccountId": src_acc_id,
            "DstAccountId": dst_acc_id,
            "TransactionId": str(tx_id),
            "BookingDateTime": now,
            "Type": type,
            "Result": False
        }

        if int(tx_amount) <= 0:
            print("ERROR: Invalid amount")
            tx_resp["ErrorMsg"] = "ERROR: Invalid amount"
            return tx_resp

        if src_acc_id == dst_acc_id:
            print("ERROR: Same source and destination accounts not allowed")
            tx_resp["ErrorMsg"] = "ERROR: Same source and destination accounts not allowed"
            return tx_resp


        print("SOURCE ACC: {}".format(src_acc_id))

        try:
            r = self._ddb_cli.transact_write_items(
                TransactItems=[
                    {
                        'Put': {
                            'TableName': TABLE_NAME,
                            'Item': {
                                "id": { "S": src_acc_id },
                                "sort": { "S" : "TX_{}".format(now)},
                                "Amount": { "N": str(tx_amount) },
                                "SrcAccountId": { "S": src_acc_id },
                                "DstAccountId": { "S": dst_acc_id },
                                "CreditDebitIndicator": { "S": "Debit"},
                                "TransactionId":  { "S": tx_id },
                                "BookingDateTime": { "S": now },
                                "Type": { "S": type }
                            },
                            'ReturnValuesOnConditionCheckFailure': 'NONE'
                        }
                    },
                    {
                        'Put': {
                            'TableName': TABLE_NAME,
                            'Item': {
                                "id": { "S": dst_acc_id },
                                "sort": { "S" : "TX_{}".format(now)},
                                "Amount": { "N": str(tx_amount) },
                                "SrcAccountId": { "S": src_acc_id },
                                "DstAccountId": { "S": dst_acc_id },
                                "CreditDebitIndicator": { "S": "Credit"},
                                "TransactionId":  { "S": tx_id },
                                "BookingDateTime": { "S": now },
                                "Type": { "S": type }
                            },
                            'ReturnValuesOnConditionCheckFailure': 'NONE'
                        }
                    },
                    {
                        'Update': {
                            'TableName': TABLE_NAME,
                            'Key': { 'id': { "S": src_acc_id }, 'sort': { "S": "BALANCE"} },
                            'ConditionExpression': '#amount >= :tx_amount',
                            'UpdateExpression': 'SET #amount = #amount - :tx_amount',
                            'ExpressionAttributeNames': {
                                '#amount': 'Amount'
                            },
                            'ExpressionAttributeValues': {
                                ':tx_amount': { "N": str(tx_amount) }
                            },
                            'ReturnValuesOnConditionCheckFailure': 'NONE'
                        }
                    },
                    {
                        'Update': {
                            'TableName': TABLE_NAME,
                            'Key': { 'id': { "S": dst_acc_id }, 'sort': { "S": "BALANCE"} },
                            #'ConditionExpression': '#amount <= :tx_amount',
                            'UpdateExpression': 'SET #amount = #amount + :tx_amount',
                            'ExpressionAttributeNames': {
                                '#amount': 'Amount'
                            },
                            'ExpressionAttributeValues': {
                                ':tx_amount': { "N": str(tx_amount) }
                            },
                            'ReturnValuesOnConditionCheckFailure': 'NONE'
                        }
                    }
                ],
                ReturnConsumedCapacity="TOTAL",
                ReturnItemCollectionMetrics="SIZE"
            )

            if r["ResponseMetadata"]["HTTPStatusCode"] == 200:
                tx_resp["Result"] = True
            else:
                print("####\n{}".format(json.dumps(r, indent=2, cls=DecimalEncoder)))
                tx_resp["ErrorMsg"] = r

        except Exception as e:
            print(e)
            tx_resp["ErrorMsg"] = str(e)

        return tx_resp


    def query_statement(self, acc_id):
        print(">query_statement")

        q = self._table.query(
            KeyConditionExpression="id = :pk AND begins_with(sort, :metadata)",
            ExpressionAttributeValues={
                ":pk": "{}".format(acc_id),
                ":metadata": "TX_"
            },
            ScanIndexForward=True,
            ReturnConsumedCapacity="TOTAL"
        )

        print(q["ConsumedCapacity"])

        return q

    def get_balance(self, acc_id):
        print(">get_balance")

        q = self._table.query(
            KeyConditionExpression="id = :pk AND begins_with(sort, :metadata)",
            ExpressionAttributeValues={
                ":pk": "{}".format(acc_id),
                ":metadata": "BALANCE"
            },
            ScanIndexForward=True
        )

        response = {
            "AccountId": acc_id,
            "Balance": 0,
            "CreditDebitIndicator": "Credit",
            "Result": False
        }

        if "Items" in q:
            if "Amount" in q["Items"][0]:
                response["Balance"] = q["Items"][0]["Amount"]
                response["Result"] = True
        else:
            print("No account data found!")
            response["ErrorMsg"] = "No account data found!"

        return response

    def query_company_by_minute(self, company_id):
        q = self._table.query(
            KeyConditionExpression="Pk = :pk AND begins_with(Sk, :sk)",
            ExpressionAttributeValues={
                ":pk": "{}".format(company_id),
                ":sk": "m#"
            },
            ConsistentRead=False,
            ScanIndexForward=True,
            ReturnConsumedCapacity="TOTAL"
        )

        print(q["ConsumedCapacity"])

        return q


    def query_company_by_minute_range(self, company_id, init, end):
        q = self._table.query(
            KeyConditionExpression="Pk = :pk AND Sk BETWEEN :init AND :end",
            ExpressionAttributeValues={
                ":pk": "{}".format(company_id),
                ":init": "m#{}".format(init),
                ":end": "m#{}".format(end)
            },
            ConsistentRead=False,
            ScanIndexForward=True,
            ReturnConsumedCapacity="TOTAL"
        )

        print(q["ConsumedCapacity"])

        return q


    def query_company_by_day(self, company_id):
        q = self._table.query(
            KeyConditionExpression="Pk = :pk AND begins_with(Sk, :sk)",
            ExpressionAttributeValues={
                ":pk": "{}".format(company_id),
                ":sk": "D#"
            },
            ConsistentRead=False,
            ScanIndexForward=True,
            ReturnConsumedCapacity="TOTAL"
        )

        print(q["ConsumedCapacity"])

        return q

    def query_company_by_month(self, company_id):
        q = self._table.query(
            KeyConditionExpression="Pk = :pk AND begins_with(Sk, :sk)",
            ExpressionAttributeValues={
                ":pk": "{}".format(company_id),
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
            "Pk": data["company_id"],
            "Sk": "M#{}".format(data["date"]),
            "FatLiq": data["fat_liquido"],
            "QtdVendas": data["qtd_vendas"],
            "TicketMedio": data["tkt_medio"],
            "FatBruto": data["fat_bruto"],
            "Desconto": data["desconto"],
            "Itens": data["itens"],
            "QtdAtendimentos": data["qtd_atendimentos"],
            "ValorTotal": data["valor_total"]
        })


    def insert_new_minute_record(self, data):
        logging.debug(">insert_new_minute_record")

        self._table.put_item(Item={
            "Pk": data["company_id"],
            "Sk": "m#{}".format(data["date"]),
            "FatLiq": data["fat_liquido"],
            "QtdVendas": data["qtd_vendas"],
            "TicketMedio": data["tkt_medio"],
            "FatBruto": data["fat_bruto"],
            "Desconto": data["desconto"],
            "Itens": data["itens"],
            "QtdAtendimentos": data["qtd_atendimentos"],
            "ValorTotal": data["valor_total"]
        })

    def insert_new_day_record(self, data):
        logging.debug(">insert_new_day_record")

        self._table.put_item(Item={
            "Pk": data["company_id"],
            "Sk": "D#{}".format(data["date"]),
            "FatLiq": data["fat_liquido"],
            "QtdVendas": data["qtd_vendas"],
            "TicketMedio": data["tkt_medio"],
            "FatBruto": data["fat_bruto"],
            "Desconto": data["desconto"],
            "Itens": data["itens"],
            "QtdAtendimentos": data["qtd_atendimentos"],
            "ValorTotal": data["valor_total"]
        })

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
    elif period == "M":
        data["date"] = random_month_formatted()
    else:
        data["date"] = random_hour_minute_formatted()

    data["company_id"] = random.choice(COMPANY)
    data["fat_liquido"] = random.randint(0, 1000)
    data["qtd_vendas"] = random.randint(0, 100)
    data["tkt_medio"] = random.randint(1, 50)
    data["fat_bruto"] = random.randint(1, 1000)
    data["desconto"] = random.randint(0, 1000)
    data["itens"] = random.randint(0, 1000)
    data["qtd_atendimentos"] = random.randint(0, 1000)
    data["valor_total"] =random.randint(0, 1000)

    return data

def load_days(n):
    logging.info(">load_data {}".format(n))

    dm = DashboardModel(table_name=TABLE_NAME)
    
    for i in range(1, 100):
        data = generate_random_data()
        dm.insert_new_day_record(data)
        #companies.add(data["company_id"])
        
        if i % 50 == 0:
            logging.info("THREAD {} I {}".format(n, i))


def load_hour_min(n):
    logging.info(">load_data {}".format(n))

    dm = DashboardModel(table_name=TABLE_NAME)
    
    for i in range(1, 100):
        data = generate_random_data(period="mm")
        dm.insert_new_minute_record(data)
        #companies.add(data["company_id"])
        
        if i % 50 == 0:
            logging.info("THREAD {} I {}".format(n, i))

if __name__ == "__main__":
    dm = DashboardModel(table_name=TABLE_NAME)
    
    print(random_hour_minute_formatted())

    companies = set()

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


    for cpy in COMPANY:
        start = time.time()
        #q = dm.query_company_by_minute(cpy)
        q = dm.query_company_by_minute_range(cpy, "2015-11-01", "2019-11-08")
        
        tkt_medio = 0
        fat_liquido = 0
        qtd_vendas = 0
        fat_bruto = 0
        recs = 0

        for items in q['Items']:
            #print(items)
            tkt_medio += items["TicketMedio"]
            fat_liquido += items["FatLiq"]
            qtd_vendas += items["QtdVendas"]
            fat_bruto += items["FatBruto"]
            recs += 1
            #"Desconto": data["desconto"],
            #"Itens": data["itens"],
            #"QtdAtendimentos": data["qtd_atendimentos"],
            #"ValorTotal": data["valor_total"]

        #print(q)
        end = time.time()
        print(end - start)
        print("\n####\nCOMPANY {}\nTKT_MEDIO {} FAT_LIQ {} QTD_VENDAS {}\nRECS {}".format(cpy, tkt_medio, fat_liquido, qtd_vendas, recs))

