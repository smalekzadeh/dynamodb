from __future__ import print_function

import boto3
import re
from boto3.dynamodb.conditions import Key, Attr
import time
import datetime
from collections import defaultdict
from collections import Counter
from datetime import datetime
import decimal
from decimal import Decimal
import calendar

dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

table = dynamodb.Table("callstab3")
#  query a index

#  query a index
print ('Query an account calls start time :', time.strftime("%H:%M:%S"))




month_account_call_count = defaultdict(int)
year_account_call_count = defaultdict(int)
hour_account_call_count = defaultdict(int)
day_account_call_count = defaultdict(int)


response = table.query(
	IndexName='accountid-calldate-index',
	KeyConditionExpression=Key('accountid').eq("ACC-1230") , #& Key('calldate').between(1481205344, 1481275607)
	# FilterExpression=Attr('calltype').eq("mobile")
)


rowcount = response['Count']

for record in response["Items"]:    
		try:
			account = record["accountid"]
		except:
			account = 'NULL'
		# location
		try:
			location = record["location"]
		except:
			location = 'UNSPECIFIED'
		

		hour_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H"),"%Y-%m-%dT%H").utctimetuple()) # datetime.now().strftime("%Y-%m-%dT%H")
		day_event_time=	 calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d"),"%Y-%m-%d").utctimetuple())	#datetime.now().strftime("%Y-%m-%d")# week_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%YW%W"),"%YW%W").utctimetuple())	#datetime.now().strftime("%YW%W")
		month_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m"),"%Y-%m").utctimetuple())	#datetime.now().strftime("%Y-%m")
		year_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y"),"%Y").utctimetuple())#datetime.now().strftime("%Y")


		hour_account_call_count[(account, location,hour_event_time)] += 1
		day_account_call_count[(account, location,day_event_time)] += 1
		month_account_call_count[(account,location, month_event_time)] += 1
		year_account_call_count[(account, location,year_event_time)] += 1


while 'LastEvaluatedKey' in response:
	response = table.query(
		IndexName='accountid-calldate-index',
		KeyConditionExpression=Key('accountid').eq("ACC-1230") , #& Key('calldate').between(1481205344, 1481275607)
		FilterExpression=Attr('calltype').eq("mobile")
	)


	rowcount = rowcount + response['Count']

	for record in response["Items"]:    
			try:
				account = record["accountid"]
			except:
				account = 'NULL'
			# location
			try:
				location = record["location"]
			except:
				location = 'UNSPECIFIED'
			

			hour_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H"),"%Y-%m-%dT%H").utctimetuple()) # datetime.now().strftime("%Y-%m-%dT%H")
			day_event_time=	 calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d"),"%Y-%m-%d").utctimetuple())	#datetime.now().strftime("%Y-%m-%d")# week_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%YW%W"),"%YW%W").utctimetuple())	#datetime.now().strftime("%YW%W")
			month_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m"),"%Y-%m").utctimetuple())	#datetime.now().strftime("%Y-%m")
			year_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y"),"%Y").utctimetuple())#datetime.now().strftime("%Y")


			hour_account_call_count[(account, location,hour_event_time)] += 1
			day_account_call_count[(account, location,day_event_time)] += 1
			month_account_call_count[(account,location, month_event_time)] += 1
			year_account_call_count[(account, location,year_event_time)] += 1




print ('Query an account calls end time :', time.strftime("%H:%M:%S"))
print('total record count: ' , rowcount)	
print('hour record count: ' , len(hour_account_call_count))
print('day record count: ' , len(day_account_call_count))	
print('month record count: ' , len(month_account_call_count))	
print('year record count: ' , len(year_account_call_count))				


# for key,val in hour_account_call_count.iteritems():
# 	print("%s, %s , %s= %s" % (key[0], key[1], key[2],val))

