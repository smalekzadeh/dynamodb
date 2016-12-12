from __future__ import print_function # Python 2/3 compatibility
import boto3


def deletetable(tablename):
    dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

    table = dynamodb.Table(tablename)

    response = table.delete()

def createtable(tablename):
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')

    table = dynamodb.create_table(
        TableName=tablename,
        KeySchema=[
            {
                'AttributeName': 'callid',
                'KeyType': 'HASH'  #Partition key
            },
            
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'callid',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'accountid',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'trunkid',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'location',
                'AttributeType': 'S'
            },
             {
                'AttributeName': 'source',
                'AttributeType': 'S'
            },
            { 
                'AttributeName': 'calldate',
                'AttributeType': 'N'
            },

        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'accountid-calldate-index',
                'KeySchema': [
                    {
                        'AttributeName': 'accountid',
                        'KeyType': 'HASH'  #Partition key
                    },
                    {
                        'AttributeName': 'calldate',
                        'KeyType': 'RANGE'  #Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                                        'ReadCapacityUnits': 5,
                                        'WriteCapacityUnits': 30
                                    }
            },
            {
                'IndexName': 'trunkid-calldate-index',
                'KeySchema': [
                    {
                        'AttributeName': 'trunkid',
                        'KeyType': 'HASH'  #Partition key
                    },
                    {
                        'AttributeName': 'calldate',
                        'KeyType': 'RANGE'  #Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                                        'ReadCapacityUnits': 5,
                                        'WriteCapacityUnits': 30
                                    }
            },
            {
                'IndexName': 'location-calldate-index',
                'KeySchema': [
                    {
                        'AttributeName': 'location',
                        'KeyType': 'HASH'  #Partition key
                    },
                    {
                        'AttributeName': 'calldate',
                        'KeyType': 'RANGE'  #Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                                        'ReadCapacityUnits': 5,
                                        'WriteCapacityUnits': 30
                                    }
            },
            {
                'IndexName': 'source-calldate-index',
                'KeySchema': [
                    {
                        'AttributeName': 'source',
                        'KeyType': 'HASH'  #Partition key
                    },
                    {
                        'AttributeName': 'calldate',
                        'KeyType': 'RANGE'  #Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                                        'ReadCapacityUnits': 5,
                                        'WriteCapacityUnits': 30
                                    }
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 30,
        },
        StreamSpecification={
            'StreamEnabled': True,
            'StreamViewType': 'NEW_IMAGE'
        }
    )

# print("Table status:", table.table_status)

if __name__ == '__main__':
    var = ""
    print("table name  for the table to be deleted:")
    var = raw_input("")
    if not var == "":
        deletetable(var)
    print("table name  for the table to be created:")
    var = raw_input("")
    if not var == "":
        createtable(var)
