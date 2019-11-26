
import boto3
import os

table_name = os.getenv("TABLE_NAME")

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)

scan = table.scan(
    ProjectionExpression='#pk, #sk',
    ExpressionAttributeNames={
        '#pk': 'Pk',
        '#sk': 'Sk'
    }
)

i = 0
with table.batch_writer() as batch:
    for each in scan['Items']:
        if i % 100 == 0:
            print("{}: {}".format(i, each))
        batch.delete_item(Key={ "Pk": each["Pk"], "Sk": each["Sk"]})
        i+=1