from __future__ import print_function

import boto3
import re
from boto3.dynamodb.conditions import Key
# import time
import datetime
# from collections import defaultdict
# from collections import Counter
from datetime import datetime
# import decimal
# from decimal import Decimal
# import calendar

		
		
# update the account  call counts
def update_account_call_counter(account, location, calltype, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('call_stats')
		
		
		
		# insert the item
		response = table.update_item(
		Key={
			'entityid': account, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD AA_TOTALS_COUNT :value, %s :value , %s :value" % (location, calltype)
		)
 
# update the trunk call counts
def update_trunk_call_counter(trunk, location, calltype, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('call_stats')
		
		
		
		# insert the item
		response = table.update_item(
		Key={
			'entityid': trunk, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD AA_TOTALS_COUNT :value, %s :value , %s :value" % (location, calltype)
		)
# update the trunk call counts
def update_source_call_counter(source, location, calltype, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('call_stats')	
		
		# insert the item
		response = table.update_item(
		Key={
			'entityid': source, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD AA_TOTALS_COUNT :value, %s :value , %s :value" % (location, calltype)
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
			
        # print(record)
        
        
		calldate = record['dynamodb']["NewImage"]["calldate"]["N"]
		


		hour_event_time = datetime.strftime(datetime.strptime(calldate,"%Y%m%d%H%M%S"),"%Y%m%d%H")
		day_event_time  = datetime.strftime(datetime.strptime(calldate,"%Y%m%d%H%M%S"),"%Y%m%d")
		month_event_time = datetime.strftime(datetime.strptime(calldate,"%Y%m%d%H%M%S"),"%Y%m")
		year_event_time = datetime.strftime(datetime.strptime(calldate,"%Y%m%d%H%M%S"),"%Y")
        
		update_account_call_counter( account, location,calltype,int(hour_event_time), 1)
		update_account_call_counter( account, location,calltype,int(day_event_time), 1)
		update_account_call_counter( account, location,calltype,int(month_event_time), 1)
		update_account_call_counter( account, location,calltype,int(year_event_time), 1)
		
		update_trunk_call_counter( trunk, location,calltype,int(hour_event_time), 1)
		update_trunk_call_counter( trunk, location,calltype,int(day_event_time), 1)
		update_trunk_call_counter( trunk, location,calltype,int(month_event_time), 1)
		update_trunk_call_counter( trunk, location,calltype,int(year_event_time), 1)
		
		update_source_call_counter( source, location,calltype,int(hour_event_time), 1)
		update_source_call_counter( source, location,calltype,int(day_event_time), 1)
		update_source_call_counter( source, location,calltype,int(month_event_time), 1)
		update_source_call_counter( source, location,calltype,int(year_event_time), 1)

	donetime = datetime.now()
	print("setting dict: " , donetime - timenow)

	return True