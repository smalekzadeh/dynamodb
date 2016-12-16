from __future__ import print_function
import boto3
import re
from boto3.dynamodb.conditions import Key
import datetime
from datetime import datetime

		
		
# update the account  call counts
def update_account_call_counter(account, location, calltype, duration, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('call_stats')
		
		locationcount = location + "_count"
		locationdur =  location + "_duration"

		calltypecount = calltype + "_count"
		calltypedur = calltype + "_duration"
		
		# insert the item
		response = table.update_item(
		Key={
			'entityid': account, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count, ":duration":duration},
		UpdateExpression="ADD AA_TOTALS_COUNT :value, AA_TOTALS_DUR :duration,  %s :value , %s :value , %s :duration, %s :duration" % (locationcount, calltypecount,locationdur,calltypedur)
		)
 
# update the trunk call counts
def update_trunk_call_counter(trunk, location, calltype, duration, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('call_stats')
		
		locationcount = location + "_count"
		locationdur =  location + "_duration"

		calltypecount = calltype + "_count"
		calltypedur = calltype + "_duration"
		
		
		# insert the item
		response = table.update_item(
		Key={
			'entityid': trunk, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count, ":duration":duration},
		UpdateExpression="ADD AA_TOTALS_COUNT :value, AA_TOTALS_DUR :duration,  %s :value , %s :value , %s :duration, %s :duration" % (locationcount, calltypecount,locationdur,calltypedur)
		)

# update the trunk call counts
def update_source_call_counter(source, location, calltype, duration, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('call_stats')	
		
		locationcount = location + "_count"
		locationdur =  location + "_duration"

		calltypecount = calltype + "_count"
		calltypedur = calltype + "_duration"
		
		# insert the item
		response = table.update_item(
		Key={
			'entityid': source, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count, ":duration":duration},
		UpdateExpression="ADD AA_TOTALS_COUNT :value, AA_TOTALS_DUR :duration,  %s :value , %s :value , %s :duration, %s :duration" % (locationcount, calltypecount,locationdur,calltypedur)
		)
		
		
def lambda_handler(event, context):

	timenow = datetime.now()
	print("number of records in this lamda call: ", len(event['Records']))
# 	print(event['Records'])
	for record in event['Records']:    
		# get the variables from the event image
		# account
		try:
			account = record['dynamodb']["NewImage"]["accountid"]["S"]
		except:
			account = 'NULL'
		# trunk
		try:
			trunk = record['dynamodb']["NewImage"]["trunkid"]["S"]
		except:
			trunk = 'NULL'
		# source
		try:
			source = record['dynamodb']["NewImage"]["source"]["S"]
		except:
			source = 'NULL'
# 		# location
		try:
			location = record['dynamodb']["NewImage"]["location"]["S"]
		except:
			location = 'UNSPECIFIED'
		# calltype
		try:
			calltype = record['dynamodb']["NewImage"]["calltype"]["S"]
		except:
			calltype = 'NoCallType'
		# duration
		try:
			duration = int(record['dynamodb']["NewImage"]["duration"]["N"])
		except:
			duration = 0
			
        # print(record)
        
        
		calldate = record['dynamodb']["NewImage"]["calldate"]["N"]
		


		hour_event_time = datetime.strftime(datetime.strptime(calldate,"%Y%m%d%H%M%S"),"%Y%m%d%H")
		day_event_time  = datetime.strftime(datetime.strptime(calldate,"%Y%m%d%H%M%S"),"%Y%m%d")
		month_event_time = datetime.strftime(datetime.strptime(calldate,"%Y%m%d%H%M%S"),"%Y%m")
		year_event_time = datetime.strftime(datetime.strptime(calldate,"%Y%m%d%H%M%S"),"%Y")
        
		update_account_call_counter( account, location,calltype,duration,int(hour_event_time), 1)
		update_account_call_counter( account, location,calltype,duration,int(day_event_time), 1)
		update_account_call_counter( account, location,calltype,duration,int(month_event_time), 1)
		update_account_call_counter( account, location,calltype,duration,int(year_event_time), 1)
		
		update_trunk_call_counter( trunk, location,calltype,duration,int(hour_event_time), 1)
		update_trunk_call_counter( trunk, location,calltype,duration,int(day_event_time), 1)
		update_trunk_call_counter( trunk, location,calltype,duration,int(month_event_time), 1)
		update_trunk_call_counter( trunk, location,calltype,duration,int(year_event_time), 1)
		
		update_source_call_counter( source, location,calltype,duration,int(hour_event_time), 1)
		update_source_call_counter( source, location,calltype,duration,int(day_event_time), 1)
		update_source_call_counter( source, location,calltype,duration,int(month_event_time), 1)
		update_source_call_counter( source, location,calltype,duration,int(year_event_time), 1)

	donetime = datetime.now()
	print("setting dict: " , donetime - timenow)

	return True