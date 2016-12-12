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


	week_account_call_count = defaultdict(int)
	month_account_call_count = defaultdict(int)
	year_account_call_count = defaultdict(int)
	hour_account_call_count = defaultdict(int)
	day_account_call_count = defaultdict(int)

	# set the date dorts
	hour_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%Y-%m-%dT%H"),"%Y-%m-%dT%H").utctimetuple()) # datetime.now().strftime("%Y-%m-%dT%H")
	day_event_time=	 calendar.timegm(datetime.strptime(datetime.now().strftime("%Y-%m-%d"),"%Y-%m-%d").utctimetuple())	#datetime.now().strftime("%Y-%m-%d")
	month_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%Y-%m"),"%Y-%m").utctimetuple())	#datetime.now().strftime("%Y-%m")
	year_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%Y"),"%Y").utctimetuple())#datetime.now().strftime("%Y")



	timenow = datetime.now()
	print("number of records in this lamda call: ", len(event['Records']))
	for record in event['Records']:    
		# get the variables from the event image
		# account
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

		# hour_event_time=datetime.now().strftime("%Y-%m-%dT%H")
		# day_event_time=datetime.now().strftime("%Y-%m-%d")
		# week_event_time=datetime.now().strftime("%YW%W")
		# month_event_time=datetime.now().strftime("%Y-%m")
		# year_event_time=datetime.now().strftime("%Y")

		update_source_call_counter( source, location,calltype,hour_event_time, 1)
		update_source_call_counter( source, location,calltype,day_event_time, 1)
		update_source_call_counter( source, location,calltype,month_event_time, 1)
		update_source_call_counter( source, location,calltype,year_event_time, 1)


	donetime = datetime.now()
	print("setting dict: " , donetime - timenow)

	return True