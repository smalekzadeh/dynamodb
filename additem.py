import json
import boto3
from datetime import datetime
from collections import defaultdict


# update the events hourly call counts
def update_dynamo_event_counter(tableName, event_name, event_datetime, event_count=1, dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
    print "hello1234"
    table = dynamodb.Table(tableName)
    print(table)
    response = table.update_item(
        Key={
            'EventName': event_name,
            'EventHour': event_datetime
        },
        ExpressionAttributeValues={":value": event_count},
        UpdateExpression="ADD EventCount :value")
    print "done"


# update the account trunk hourly call counts
def update_account_trunk_call_counter_hour(tableName, account_name, trunk_name, event_datetime, event_count=1, dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
    table = dynamodb.Table(tableName)
    print("++++++++       ++++++++")
    print(trunk_name)


    response = table.update_item(
        Key={
            'account': account_name,
            'call_hour': event_datetime
        },
        UpdateExpression="SET trunk = :value1",
        ExpressionAttributeValues={":value1": trunk_name})

    response = table.update_item(
        Key={
            'account': account_name,
            'call_hour': event_datetime
        },
        ExpressionAttributeValues={":value": event_count},
        UpdateExpression="ADD CallCount :value")

if __name__ == "__main__":

    # update_dynamo_event_counter('call_event_counter', 'TEST1234', '1234567', 1)
    update_account_trunk_call_counter_hour(
        'account_trunk_call_count_hour', '5b55ab07-aa24-49bc-beed-2e5f88f1af56', '3000', "2016-11-21T10", 1)
