from __future__ import print_function

import boto3
import re
from boto3.dynamodb.conditions import Key
import time
import datetime
from collections import defaultdict
from collections import Counter
from datetime import datetime
import decimal
from decimal import Decimal

		
# update the account  call counts
def update_account_call_counter(account, location, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('account_call_count')
		
		
		
		# insert the item
		response = table.update_item(
		Key={
			'accountid': account, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD TOTALS_COUNT :value, %s :value" % location
		)
 

def lambda_handler(event, context):


	week_account_call_count = defaultdict(int)
	month_account_call_count = defaultdict(int)
	year_account_call_count = defaultdict(int)
	hour_account_call_count = defaultdict(int)
	day_account_call_count = defaultdict(int)


	timenow = datetime.now()
	for record in event['Records']:    
		# get the variables from the event image
		# account
		try:
			account = record['dynamodb']["NewImage"]["accountid"]["S"]
		except:
			account = 'NULL'
# 		# location
		try:
			location = record['dynamodb']["NewImage"]["location"]["S"]
			location = re.sub(r'([^\s\w]|_)+', '', location).replace(' ', '_')
		except:
			location = 'UNSPECIFIED'

		hour_event_time=datetime.now().strftime("%Y-%m-%dT%H")
		day_event_time=datetime.now().strftime("%Y-%m-%d")
		week_event_time=datetime.now().strftime("%YW%W")
		month_event_time=datetime.now().strftime("%Y-%m")
		year_event_time=datetime.now().strftime("%Y")


		hour_account_call_count[(account, location,hour_event_time)] += 1
		day_account_call_count[(account, location,day_event_time)] += 1
		week_account_call_count[(account, location,week_event_time)] += 1
		month_account_call_count[(account,location, month_event_time)] += 1
		year_account_call_count[(account, location,year_event_time)] += 1


	# insert account call counts 
	for key,val in hour_account_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_account_call_counter( key[0], key[1],key[2], int(val)) 
	donetime = datetime.now()
	print(donetime - timenow)
	for key,val in day_account_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_account_call_counter(key[0], key[1],key[2], int(val)) 

	for key,val in week_account_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_account_call_counter( key[0], key[1],key[2], int(val))
	donetime = datetime.now()
	print(donetime - timenow)
	for key,val in month_account_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_account_call_counter( key[0], key[1],key[2], int(val))
	donetime = datetime.now()
	print(donetime - timenow)
	for key,val in year_account_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_account_call_counter( key[0], key[1],key[2], int(val))
	donetime = datetime.now()
	print(donetime - timenow)

	return True