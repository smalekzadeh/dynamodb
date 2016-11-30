from __future__ import print_function # Python 2/3 compatibility
import boto3

dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')


table = dynamodb.create_table(
    TableName='testtab',
    KeySchema=[
        {
            'AttributeName': 'account-trunk',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'call_hour',
            'KeyType': 'RANGE'  #Sort key
        }
        ],

    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

print("Table status:", table.table_status)