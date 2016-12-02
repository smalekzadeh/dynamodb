import boto3
from boto3.dynamodb.conditions import Key
import time
import datetime
from collections import defaultdict
from collections import Counter
from datetime import datetime
import decimal
from decimal import Decimal

def trunkcalls():
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table("callstab")

	#  query a index
	print ('Collect a trunk call count start time :', time.strftime("%H:%M:%S"))
	dd = datetime.fromtimestamp(int(Decimal('1480431567'))).strftime("%Y-%m-%dT%H")

	response = table.scan(
		IndexName='trunkid-calldate-index',
		# KeyConditionExpression=Key('trunkid').eq(trunkid),
		Limit=100000
	)

	hour_trunk_call_count = defaultdict(int)
	day_trunk_call_count = defaultdict(int)
	week_trunk_call_count = defaultdict(int)
	month_trunk_call_count = defaultdict(int)
	year_trunk_call_count = defaultdict(int)

	for record in response["Items"]:    
		trunk = record["trunkid"]
		# date and hour
		hour_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H")
		day_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d")
		week_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%YW%W")
		month_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m")
		year_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y")

		hour_trunk_call_count[(trunk, hour_event_time)] += 1
		day_trunk_call_count[(trunk, day_event_time)] += 1
		week_trunk_call_count[(trunk, week_event_time)] += 1
		month_trunk_call_count[(trunk, month_event_time)] += 1
		year_trunk_call_count[(trunk, year_event_time)] += 1


	while 'LastEvaluatedKey' in response:

		response = table.scan(
				IndexName='trunkid-calldate-index',
				ExclusiveStartKey=response['LastEvaluatedKey']
				)
		
		for record in response["Items"]:    
			trunk = record["trunkid"]
			# date and hour
			hour_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H")
			day_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d")
			week_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%YW%W")
			month_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m")
			year_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y")

			hour_trunk_call_count[(trunk, hour_event_time)] += 1
			day_trunk_call_count[(trunk, day_event_time)] += 1
			week_trunk_call_count[(trunk, week_event_time)] += 1
			month_trunk_call_count[(trunk, month_event_time)] += 1
			year_trunk_call_count[(trunk, year_event_time)] += 1
		# 

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

	print ('Collect a trunk call count end time   :', time.strftime("%H:%M:%S")) 


def accountcalls():
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table("callstab")

	#  query a index
	print ('Collect an account call count start time :', time.strftime("%H:%M:%S"))
	dd = datetime.fromtimestamp(int(Decimal('1480431567'))).strftime("%Y-%m-%dT%H")

	response = table.scan(
		IndexName='accountid-calldate-index',
		Limit=100000
	)

	hour_account_call_count = defaultdict(int)
	day_account_call_count = defaultdict(int)
	week_account_call_count = defaultdict(int)
	month_account_call_count = defaultdict(int)
	year_account_call_count = defaultdict(int)

	for record in response["Items"]:    
		account = record["accountid"]
		# date and hour
		hour_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H")
		day_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d")
		week_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%YW%W")
		month_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m")
		year_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y")

		hour_account_call_count[(account, hour_event_time)] += 1
		day_account_call_count[(account, day_event_time)] += 1
		week_account_call_count[(account, week_event_time)] += 1
		month_account_call_count[(account, month_event_time)] += 1
		year_account_call_count[(account, year_event_time)] += 1
	

	while 'LastEvaluatedKey' in response:

		response = table.scan(
				IndexName='accountid-calldate-index',
				ExclusiveStartKey=response['LastEvaluatedKey']
				)
		
		for record in response["Items"]:    
			account = record["accountid"]
			# date and hour
			hour_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H")
			day_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d")
			week_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%YW%W")
			month_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m")
			year_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y")

			hour_account_call_count[(account, hour_event_time)] += 1
			day_account_call_count[(account, day_event_time)] += 1
			week_account_call_count[(account, week_event_time)] += 1
			month_account_call_count[(account, month_event_time)] += 1
			year_account_call_count[(account, year_event_time)] += 1
		# 

	 # insert/update account call counts hourly
	for key,val in hour_account_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_account_call_counter(key[0], key[1], int(val)) 

	for key,val in day_account_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_account_call_counter(key[0], key[1], int(val)) 

	for key,val in week_account_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_account_call_counter(key[0], key[1], int(val)) 

	for key,val in month_account_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_account_call_counter(key[0], key[1], int(val)) 

	for key,val in year_account_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_account_call_counter(key[0], key[1], int(val)) 

	print ('Collect an account call count end time   :', time.strftime("%H:%M:%S")) 

def locationcalls():
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table("callstab")

	#  query a index
	print ('Collect a location call count start time :', time.strftime("%H:%M:%S"))
	dd = datetime.fromtimestamp(int(Decimal('1480431567'))).strftime("%Y-%m-%dT%H")

	response = table.scan(
		IndexName='location-calldate-index',
		Limit=100000
	)

	hour_location_call_count = defaultdict(int)
	day_location_call_count = defaultdict(int)
	week_location_call_count = defaultdict(int)
	month_location_call_count = defaultdict(int)
	year_location_call_count = defaultdict(int)

	for record in response["Items"]:    
		location = record["location"]
		# date and hour
		hour_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H")
		day_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d")
		week_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%YW%W")
		month_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m")
		year_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y")

		hour_location_call_count[(location, hour_event_time)] += 1
		day_location_call_count[(location, day_event_time)] += 1
		week_location_call_count[(location, week_event_time)] += 1
		month_location_call_count[(location, month_event_time)] += 1
		year_location_call_count[(location, year_event_time)] += 1
	

	while 'LastEvaluatedKey' in response:

		response = table.scan(
				IndexName='location-calldate-index',
				ExclusiveStartKey=response['LastEvaluatedKey']
				)
		
		for record in response["Items"]:    
			location = record["location"]
			# date and hour
			hour_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H")
			day_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d")
			week_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%YW%W")
			month_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m")
			year_event_time=datetime.fromtimestamp(int(record["calldate"])).strftime("%Y")

			hour_location_call_count[(location, hour_event_time)] += 1
			day_location_call_count[(location, day_event_time)] += 1
			week_location_call_count[(location, week_event_time)] += 1
			month_location_call_count[(location, month_event_time)] += 1
			year_location_call_count[(location, year_event_time)] += 1
		# 

	#  # insert/update account call counts hourly
	for key,val in hour_location_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_location_call_counter(key[0], key[1], int(val)) 

	for key,val in day_location_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_location_call_counter(key[0], key[1], int(val)) 

	for key,val in week_location_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_location_call_counter(key[0], key[1], int(val)) 

	for key,val in month_location_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_location_call_counter(key[0], key[1], int(val)) 

	for key,val in year_location_call_count.iteritems():
		# print("%s, %s = %s" % (str(key[0]), str(key[1]), str(val)))
		update_location_call_counter(key[0], key[1], int(val)) 

	print ('Collect a location call count end time   :', time.strftime("%H:%M:%S")) 


	
# update the trunk hourly call counts
def update_trunk_call_counter(trunk, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('trunk_call_count')
		# delete the item
		response = table.delete_item(
        Key={
            'trunkid': trunk, 
			'date_sort': event_datetime
        	}
    	)
		# insert the item
		response = table.update_item(
		Key={
			'trunkid': trunk, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD callcount :value")

		

# update the account hourly call counts
def update_account_call_counter(account, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('account_call_count')
		# delete the item
		response = table.delete_item(
        Key={
            'accountid': account, 
			'date_sort': event_datetime
        	}
    	)
		# insert the item
		response = table.update_item(
		Key={
			'accountid': account, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD callcount :value")


# update the account hourly call counts
def update_location_call_counter(location, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table('location_call_count')
		# delete the item
		response = table.delete_item(
        Key={
            'location': location, 
			'date_sort': event_datetime
        	}
    	)
		# insert the item
		response = table.update_item(
		Key={
			'location': location, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD callcount :value")


if __name__ == '__main__':
	var = ""
	while not var == "x":
		print ("")
		print ("")
		print ("Please select a query:")
		print ("")
		print ("------------------------------------------")
		print("(a) collect trunk call hour")
		print("(b) collect account call hour")
		# print("(c) collect account call hour")
		print("(x) to exit")
		print ("------------------------------------------")
		print ("")
		var = raw_input("")

		# try:
		if var == "a":
			trunkcalls()
		if var == "b":
			accountcalls()
		# if var == "c":
		# 	locationcalls()