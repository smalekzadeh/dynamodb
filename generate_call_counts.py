import boto3
from boto3.dynamodb.conditions import Key
import time
import datetime
from collections import defaultdict
from collections import Counter
from datetime import datetime
import decimal
from decimal import Decimal
import calendar


datetime.fromtimestamp(1346236702).strftime("%YW%W")



def trunkcalls(sourcetab,desttab):
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table(sourcetab)

	#  query a index
	print ('Collect a trunk call count start time :', time.strftime("%H:%M:%S"))
	dd = datetime.fromtimestamp(int(Decimal('1480431567'))).strftime("%Y-%m-%dT%H")

	response = table.scan(
		IndexName='trunkid-calldate-index',
		# KeyConditionExpression=Key('trunkid').eq(trunkid),
		Limit=100000
	)

	for record in response["Items"]:    
		try:
			trunk = record["trunkid"]
		except:
			trunk = 'NULL'
		# location
		try:
			location = record["location"]
		except:
			location = 'UNSPECIFIED'
		# calltype
		try:
			calltype = record["calltype"]
		except:
			calltype = 'NoCallType'

		hour_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H"),"%Y-%m-%dT%H").utctimetuple()) # datetime.now().strftime("%Y-%m-%dT%H")
		day_event_time=	 calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d"),"%Y-%m-%d").utctimetuple())	#datetime.now().strftime("%Y-%m-%d")# week_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%YW%W"),"%YW%W").utctimetuple())	#datetime.now().strftime("%YW%W")
		month_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m"),"%Y-%m").utctimetuple())	#datetime.now().strftime("%Y-%m")
		year_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y"),"%Y").utctimetuple())#datetime.now().strftime("%Y")

		update_trunk_call_counter( desttab,trunk, location,calltype,hour_event_time, 1)
		update_trunk_call_counter( desttab,trunk, location,calltype,day_event_time, 1)
		update_trunk_call_counter( desttab,trunk, location,calltype,month_event_time, 1)
		update_trunk_call_counter( desttab,trunk, location,calltype,year_event_time, 1)


	while 'LastEvaluatedKey' in response:

		response = table.scan(
				IndexName='trunkid-calldate-index',
				ExclusiveStartKey=response['LastEvaluatedKey']
				)
		
		for record in response["Items"]:    
			
			try:
				trunk = record["trunkid"]
			except:
				trunk = 'NULL'
			# location
			try:
				location = record["location"]
			except:
				location = 'UNSPECIFIED'
			# calltype
			try:
				calltype = record["calltype"]
			except:
				calltype = 'NoCallType'

			hour_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H"),"%Y-%m-%dT%H").utctimetuple()) # datetime.now().strftime("%Y-%m-%dT%H")
			day_event_time=	 calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d"),"%Y-%m-%d").utctimetuple())	#datetime.now().strftime("%Y-%m-%d")# week_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%YW%W"),"%YW%W").utctimetuple())	#datetime.now().strftime("%YW%W")
			month_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m"),"%Y-%m").utctimetuple())	#datetime.now().strftime("%Y-%m")
			year_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y"),"%Y").utctimetuple())#datetime.now().strftime("%Y")

			update_trunk_call_counter( desttab,trunk, location,calltype,hour_event_time, 1)
			update_trunk_call_counter( desttab,trunk, location,calltype,day_event_time, 1)
			update_trunk_call_counter( desttab,trunk, location,calltype,month_event_time, 1)
			update_trunk_call_counter( desttab,trunk, location,calltype,year_event_time, 1)
		# 

	print ('Collect a trunk call count end time   :', time.strftime("%H:%M:%S")) 


# update the trunk call counts
def update_trunk_call_counter(desttab,trunk, location, calltype, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table(desttab)
		# insert the item
		response = table.update_item(
		Key={
			'entityid': trunk, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD AA_TOTALS_COUNT :value, %s :value , %s :value" % (location, calltype)
		)

def accountcalls(sourcetab,desttab):
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table(sourcetab)

	#  query a index
	print ('Collect an account call count start time :', time.strftime("%H:%M:%S"))
	dd = datetime.fromtimestamp(int(Decimal('1480431567'))).strftime("%Y-%m-%dT%H")

	response = table.scan(
		IndexName='accountid-calldate-index',
		Limit=100000
	)


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
		# calltype
		try:
			calltype = record["calltype"]
		except:
			calltype = 'NoCallType'

		hour_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H"),"%Y-%m-%dT%H").utctimetuple()) # datetime.now().strftime("%Y-%m-%dT%H")
		day_event_time=	 calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d"),"%Y-%m-%d").utctimetuple())	#datetime.now().strftime("%Y-%m-%d")# week_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%YW%W"),"%YW%W").utctimetuple())	#datetime.now().strftime("%YW%W")
		month_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m"),"%Y-%m").utctimetuple())	#datetime.now().strftime("%Y-%m")
		year_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y"),"%Y").utctimetuple())#datetime.now().strftime("%Y")

		update_account_call_counter( desttab,account, location,calltype,hour_event_time, 1)
		update_account_call_counter( desttab,account, location,calltype,day_event_time, 1)
		update_account_call_counter( desttab,account, location,calltype,month_event_time, 1)
		update_account_call_counter( desttab,account, location,calltype,year_event_time, 1)




	while 'LastEvaluatedKey' in response:

		response = table.scan(
				IndexName='accountid-calldate-index',
				ExclusiveStartKey=response['LastEvaluatedKey']
				)
		
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
			# calltype
			try:
				calltype = record["calltype"]
			except:
				calltype = 'NoCallType'

			hour_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H"),"%Y-%m-%dT%H").utctimetuple()) # datetime.now().strftime("%Y-%m-%dT%H")
			day_event_time=	 calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d"),"%Y-%m-%d").utctimetuple())	#datetime.now().strftime("%Y-%m-%d")# week_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%YW%W"),"%YW%W").utctimetuple())	#datetime.now().strftime("%YW%W")
			month_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m"),"%Y-%m").utctimetuple())	#datetime.now().strftime("%Y-%m")
			year_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y"),"%Y").utctimetuple())#datetime.now().strftime("%Y")

			update_account_call_counter( desttab,account, location,calltype,hour_event_time, 1)
			update_account_call_counter( desttab,account, location,calltype,day_event_time, 1)
			update_account_call_counter( desttab,account, location,calltype,month_event_time, 1)
			update_account_call_counter( desttab,account, location,calltype,year_event_time, 1)


	print ('Collect an account call count end time   :', time.strftime("%H:%M:%S")) 


# update the account  call counts
def update_account_call_counter(desttab,account, location, calltype, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table(desttab)
		
		
		
		# insert the item
		response = table.update_item(
		Key={
			'entityid': account, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD AA_TOTALS_COUNT :value, %s :value , %s :value" % (location, calltype)
		)
 


def sourcecalls(sourcetab,desttab):
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table(sourcetab)

	#  query a index
	print ('Collect source call count start time :', time.strftime("%H:%M:%S"))
	dd = datetime.fromtimestamp(int(Decimal('1480431567'))).strftime("%Y-%m-%dT%H")

	response = table.scan(
		IndexName='source-calldate-index',
		Limit=100000
	)

	for record in response["Items"]:    
		try:
			source = record["source"]
		except:
			source = 'NULL'
		# location
		try:
			location = record["location"]
		except:
			location = 'UNSPECIFIED'
		# calltype
		try:
			calltype = record["calltype"]
		except:
			calltype = 'NoCallType'

		hour_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H"),"%Y-%m-%dT%H").utctimetuple()) # datetime.now().strftime("%Y-%m-%dT%H")
		day_event_time=	 calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d"),"%Y-%m-%d").utctimetuple())	#datetime.now().strftime("%Y-%m-%d")# week_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%YW%W"),"%YW%W").utctimetuple())	#datetime.now().strftime("%YW%W")
		month_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m"),"%Y-%m").utctimetuple())	#datetime.now().strftime("%Y-%m")
		year_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y"),"%Y").utctimetuple())#datetime.now().strftime("%Y")

		update_source_call_counter( desttab,source, location,calltype,hour_event_time, 1)
		update_source_call_counter( desttab,source, location,calltype,day_event_time, 1)
		update_source_call_counter( desttab,source, location,calltype,month_event_time, 1)
		update_source_call_counter( desttab,source, location,calltype,year_event_time, 1)
	

	while 'LastEvaluatedKey' in response:

		response = table.scan(
				IndexName='source-calldate-index',
				ExclusiveStartKey=response['LastEvaluatedKey']
				)
		
		for record in response["Items"]:    
			try:
				source = record["source"]
			except:
				source = 'NULL'
			# location
			try:
				location = record["location"]
			except:
				location = 'UNSPECIFIED'
			# calltype
			try:
				calltype = record["calltype"]
			except:
				calltype = 'NoCallType'

			hour_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%dT%H"),"%Y-%m-%dT%H").utctimetuple()) # datetime.now().strftime("%Y-%m-%dT%H")
			day_event_time=	 calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m-%d"),"%Y-%m-%d").utctimetuple())	#datetime.now().strftime("%Y-%m-%d")# week_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%YW%W"),"%YW%W").utctimetuple())	#datetime.now().strftime("%YW%W")
			month_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y-%m"),"%Y-%m").utctimetuple())	#datetime.now().strftime("%Y-%m")
			year_event_time= calendar.timegm(datetime.strptime(datetime.fromtimestamp(int(record["calldate"])).strftime("%Y"),"%Y").utctimetuple())#datetime.now().strftime("%Y")

			update_source_call_counter( desttab,source, location,calltype,hour_event_time, 1)
			update_source_call_counter( desttab,source, location,calltype,day_event_time, 1)
			update_source_call_counter( desttab,source, location,calltype,month_event_time, 1)
			update_source_call_counter( desttab,source, location,calltype,year_event_time, 1)


	print ('Collect a source call count end time   :', time.strftime("%H:%M:%S")) 



 # update the trunk call counts
def update_source_call_counter(desttab,source, location, calltype, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
		table = dynamodb.Table(desttab)	
		
		# insert the item
		response = table.update_item(
		Key={
			'entityid': source, 
			'date_sort': event_datetime
		},
		ExpressionAttributeValues={":value":event_count},
		UpdateExpression="ADD AA_TOTALS_COUNT :value, %s :value , %s :value" % (location, calltype)
		)



if __name__ == '__main__':
	sourcetab = ""
	desttab = ""
	print ("")
	print ("------------------------------------------")
	print("calls table:")
	sourcetab = raw_input("")
	print("counts table:")
	desttab = raw_input("")

	trunkcalls(sourcetab,desttab)
	accountcalls(sourcetab,desttab)
	sourcecalls(sourcetab,desttab)