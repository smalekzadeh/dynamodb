from __future__ import print_function

import boto3
from boto3.dynamodb.conditions import Key
import time
import datetime
from collections import defaultdict
from collections import Counter
from datetime import datetime
import decimal
from decimal import Decimal

# update the trunk hourly call counts
def update_trunk_call_counter(trunk, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('trunk_call_count')
		
		# insert the item
		response = table.update_item(
		Key={
			'trunkid': trunk, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD callcount :value")

		

def lambda_handler(event, context):
	week_trunk_call_count = defaultdict(int)
	month_trunk_call_count = defaultdict(int)
	year_trunk_call_count = defaultdict(int)
	hour_trunk_call_count = defaultdict(int)
	day_trunk_call_count = defaultdict(int)
	for record in event['Records']:    
		
		# get the variables from the event image
		# trunk
		try:
			trunk = record['dynamodb']["NewImage"]["trunkid"]["S"]
		except:
			trunk = 'NULL'

		hour_event_time=datetime.now().strftime("%Y-%m-%dT%H")
		day_event_time=datetime.now().strftime("%Y-%m-%d")
		week_event_time=datetime.now().strftime("%YW%W")
		month_event_time=datetime.now().strftime("%Y-%m")
		year_event_time=datetime.now().strftime("%Y")

		hour_trunk_call_count[(trunk, hour_event_time)] += 1
		day_trunk_call_count[(trunk, day_event_time)] += 1
		week_trunk_call_count[(trunk, week_event_time)] += 1
		month_trunk_call_count[(trunk, month_event_time)] += 1
		year_trunk_call_count[(trunk, year_event_time)] += 1

	# insert/update trunk call counts hourly
	for key,val in hour_trunk_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_trunk_call_counter( key[0], key[1], int(val)) 

	for key,val in day_trunk_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_trunk_call_counter(key[0], key[1], int(val)) 

	for key,val in week_trunk_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_trunk_call_counter( key[0], key[1], int(val))

	for key,val in month_trunk_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_trunk_call_counter( key[0], key[1], int(val))

	for key,val in year_trunk_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_trunk_call_counter( key[0], key[1], int(val))