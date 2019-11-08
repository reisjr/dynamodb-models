import boto3
import logging
import os
import random
import json
import datetime
import decimal
from string import ascii_letters
import uuid

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)
TABLE_NAME = os.getenv('TABLE_NAME', 'DEFAULT_NAME')

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class AccountModel:

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
                            "sort": { "S" : "TX#{}#{}".format(now, random.randint(0,1000)) },
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
                                "sort": { "S" : "TX#{}#{}".format(now, random.randint(1,1000))},
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


    def create_new_account(self, email, init_balance):
        print(">create_new_account")

        # TODO: Check if account exists

        self._table.put_item(Item={
            "id": email,
            "sort": "CUSTOMER_DATA",
            "Name": "Customer Name",
            "Birth": "15-11-1900",
            "Phone": "+551199999999",
            "OverdraftAllowed": False
        })
        
        self._table.put_item(Item={
            "id": email,
            "sort": "BALANCE",
            "Amount": init_balance,
            "CreditDebitIndicator": "Credit"
        })