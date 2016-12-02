from __future__ import print_function

import json
import boto3
from datetime import datetime
from collections import defaultdict
import decimal

print('Loading function')

# update the events hourly call counts
def update_dynamo_event_counter(tableName, event_name, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
        table = dynamodb.Table(tableName)
        # print(table)
        response = table.update_item(
        Key={
            'EventName': event_name, 
            'EventHour': event_datetime
        },
        ExpressionAttributeValues={":value":event_count},
        UpdateExpression="ADD EventCount :value")
        
        
# update the account trunk call counts
def update_account_trunk_call_counter(tableName, account, trunk, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
        table = dynamodb.Table(tableName)
        # print(table)
        response = table.update_item(
        Key={
            'account': account, 
            'trunk': trunk
        },
        ExpressionAttributeValues={":value":event_count},
        UpdateExpression="ADD CallCount :value")
        

# update the account hourly call counts
def update_account_call_counter(tableName, account_name, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
        table = dynamodb.Table(tableName)
        # print(table)
        response = table.update_item(
        Key={
            'account': account_name, 
            'call_hour': event_datetime
        },
        ExpressionAttributeValues={":value":event_count},
        UpdateExpression="ADD CallCount :value")
        

# update the trunk hourly call counts
def update_trunk_call_counter(tableName, trunk, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
        table = dynamodb.Table(tableName)
        print(table)
        response = table.update_item(
        Key={
            'trunk': trunk, 
            'call_hour': event_datetime
        },
        ExpressionAttributeValues={":value":event_count},
        UpdateExpression="ADD CallCount :value")
        
# update the account trunk hourly call counts
def update_account_trunk_call_counter_hour(tableName, account_name, trunk_name, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
        table = dynamodb.Table(tableName)
        print(trunk_name)
        
        response = table.update_item(
        Key={
            'account': account_name, 
            'call_hour': event_datetime
        },
        UpdateExpression="SET trunkid = :value1",
        ConditionExpression="trunkid = :value1",
        ExpressionAttributeValues={":value1": trunk_name})


        
def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    hour_event_counter = defaultdict(int)
    hour_account_call_count = defaultdict(int)
    hour_trunk_call_count = defaultdict(int)
    account_trunk_call_count= defaultdict(int)
    hour_account_trunk_call_count= defaultdict(int)
    
    for record in event['Records']:
        print(datetime.now().strftime("%Y-%m-%dT%H"))     
        
        # get the variables from the event image
        
        # account  
        try:
            account = record['dynamodb']["NewImage"]["authaccountid"]["S"]
        except:
            account = 'NULL'

        # trunk
        try:
            trunk = record['dynamodb']["NewImage"]["authtrunkid"]["S"]
        except:
            trunk = 'NULL'
        
        # event type 
        event_type = record['eventName']
        
        # date and hour
        hour_event_time=datetime.now().strftime("%Y-%m-%dT%H")
    
    hour_account_call_count[(account, hour_event_time)] += 1
    hour_trunk_call_count[(trunk, hour_event_time)] += 1
    account_trunk_call_count[(account,trunk)] += 1
    hour_account_trunk_call_count[(account, trunk, hour_event_time)] += 1
    hour_event_counter[(event_type, hour_event_time)] += 1
    
    #  # insert/update account-trunk call counts
    # for key,val in hour_account_trunk_call_count.iteritems():
    #     print("%s, %s , %s = %s" % (str(key[0]), str(key[1]), str(key[2]), str(val)))
    #     update_account_trunk_call_counter_hour('account_trunk_call_count_hour', key[0], key[1], key[2], int(val)) 
    
    # insert/update account call counts  hourly
    for key,val in hour_account_call_count.iteritems():
        print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
        update_account_call_counter('account_call_count_hour', key[0], key[1], int(val)) 
   
    # insert/update account trunk call counts
    for key,val in account_trunk_call_count.iteritems():
        print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
        update_account_trunk_call_counter('account_trunk_call_count', key[0], key[1], int(val)) 
 
    # insert/update trunk call counts hourly
    for key,val in hour_trunk_call_count.iteritems():
        print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
        update_trunk_call_counter('trunk_call_count_hour', key[0], key[1], int(val)) 

    # insert/update event counts
    for key,val in hour_event_counter.iteritems():
        print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
        update_dynamo_event_counter('call_event_counter', key[0], key[1], int(val)) 
        
        
    return 'Successfully processed {} records.'.format(len(event['Records']))