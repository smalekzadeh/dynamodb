import boto3
from boto3.dynamodb.conditions import Key
import time
import datetime
from collections import defaultdict
from collections import Counter

def trunkcalls(trunkid):
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table("calls")

	#  query a index
	print ('Query a trunk calls start time :', time.strftime("%H:%M:%S"))
	indexName='trunkid-calldate-index'
	keyConditionExpression=Key('trunkid').eq(trunkid)
	filterExpression = None

	# querttuple =(indn,kc)

	# querystr = "IndexName='trunkid-calldate-index',KeyConditionExpression=Key('trunkid').eq(TRK-Y456-7-ACC-1232),Limit=100000"
	import pdb; pdb.set_trace()
	response = table.query(IndexName= indexName, 
							KeyConditionExpression= keyConditionExpression,
							FilterExpression=filterExpression)

	


	# IndexName='trunkid-calldate-index',
	#     KeyConditionExpression=Key('trunkid').eq(trunkid),
	#     Limit=100000
	# import pdb;pdb.set_trace()

	print ('Query  a trunk calls end time   :', time.strftime("%H:%M:%S"), '(getting first 1M which is ', response['Count'], 'the limit') 

	# use the below code to scan the whole results and get the count
	rowcount = response['Count']
	
	while 'LastEvaluatedKey' in response:

		response = table.query(
			    IndexName='trunkid-calldate-index',
			    KeyConditionExpression=Key('trunkid').eq("45692"),

			    ExclusiveStartKey=response['LastEvaluatedKey']
				)
		rowcount = response['Count'] + rowcount

	    # 

	print ("Query results item count: ", rowcount)


def trunkcallsbydatesort(trunkid,fromdate,todate):
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table("callstab2")
	fd = datetime.datetime.fromtimestamp(fromdate).strftime('%Y-%m-%d %H:%M:%S')
	td = datetime.datetime.fromtimestamp(todate).strftime('%Y-%m-%d %H:%M:%S')
	#  query a index
	print ('Query a trunk calls start time :', time.strftime("%H:%M:%S"), ' (from ' , fd, ' to ', td,')')

	response = table.query(
	    IndexName='trunkid-calldate-index',
	    KeyConditionExpression=Key('trunkid').eq(trunkid) & Key('calldate').between(fromdate, todate),
	    Limit=100000
	)

	print ('Query  a trunk calls end time   :', time.strftime("%H:%M:%S"), '(getting first 1M which is ', response['Count'], 'the limit') 

def accountcalls(accountid):
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table("callstab2")

	#  query a index
	print ('Query an account calls start time :', time.strftime("%H:%M:%S"))

	response = table.query(
	    IndexName='accountid-calldate-index',
	    KeyConditionExpression=Key('accountid').eq(accountid),
	    Limit=100000
	)

	print ('Query an account calls end time   :', time.strftime("%H:%M:%S"), '(getting first 1M which is ', response['Count'], 'the limit') 


def accountcallsbydatesort(accountid,fromdate,todate):
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table("callstab2")
	fd = datetime.datetime.fromtimestamp(fromdate).strftime('%Y-%m-%d %H:%M:%S')
	td = datetime.datetime.fromtimestamp(todate).strftime('%Y-%m-%d %H:%M:%S')
	#  query a index
	print ('Query an account calls start time :', time.strftime("%H:%M:%S"), ' (from ' , fd, ' to ', td,')')

	response = table.query(
	    IndexName='accountid-calldate-index',
	    KeyConditionExpression=Key('accountid').eq(accountid) & Key('calldate').between(fromdate, todate),
	    Limit=100000
	)

	print ('Query an account calls end time   :', time.strftime("%H:%M:%S"), '(getting first 1M which is ', response['Count'], 'the limit') 


def locationcalls(location):
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table("callstab2")

	#  query a index
	print ('Query a location calls start time :' + time.strftime("%H:%M:%S"))

	response = table.query(
	    IndexName='location-calldate-index',
	    KeyConditionExpression=Key('location').eq(location),
	    Limit=100000
	)

	print ('Query a location calls end time   :' + time.strftime("%H:%M:%S"), '(getting first 1M which is ', response['Count'], 'the limit') 


def locationcallsbydatesort(location,fromdate,todate):
	dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

	table = dynamodb.Table("calls")
	fd = datetime.datetime.fromtimestamp(fromdate).strftime('%Y-%m-%d %H:%M:%S')
	td = datetime.datetime.fromtimestamp(todate).strftime('%Y-%m-%d %H:%M:%S')
	#  query a index
	print ("Query a location and sort key calls start time: " + time.strftime("%H:%M:%S") + ' (from ' + fd + ' to ' + td +')')

	response = table.query(
	    IndexName='location-calldate-index',
	    KeyConditionExpression=Key('location').eq(location) & Key('calldate').between(fromdate, todate),
	    Limit=100000
	)

	print ("Query a location and sort key calls end time:   " + time.strftime("%H:%M:%S") + ' (Item count: ' + str(response['Count']) + ' . getting the first 1M (the limit) ') 


if __name__ == '__main__':
	var = ""
	while not var == "x":
		print ("")
		print ("")
		print ("Please select a query:")
		print ("")
		print ("------------------------------------------")
		print("(a) trunk call query")
		print("(b) trunk call and date sort key query")
		print("(c) account call query")
		print("(d) account call and date sort key query")
		print("(e) location call query")
		print("(f) location call and date sort key query")
		print("(x) to exit")
		print ("------------------------------------------")
		print ("")
		var = raw_input("")

		# try:
		if var == "a":
			print("selected trunkid")
			trk = raw_input()
			trunkcalls(trk)
		if var == "b":
			print("selected trunkid")
			trk = raw_input()
			print("selected fromdate default 1480450008")
			fd = raw_input()
			if  fd =="":
				fd = 1480450008
			print("selected todate default 1480450029")
			td = raw_input()
			if  td =="":
				td = 1480450029
			trunkcallsbydatesort(trk,int(fd),int(td))
		if var == "c":
			print("selected accountid")
			acc = raw_input()
			accountcalls(acc)
		if var == "d":
			print("selected accountid")
			acc = raw_input()
			print("selected fromdate default 1480450008")
			fd = raw_input()
			if  fd =="":
				fd = 1480450008
			print("selected todate default 1480450029")
			td = raw_input()
			if  td =="":
				td = 1480450029
			accountcallsbydatesort(acc,int(fd),int(td))
		if var == "e":
			print("selected location default Port Kelsey")
			loc = raw_input()
			if loc == "":
				loc = "Port Kelsey"
			locationcalls(loc)
		if var == "f":
			print("selected location default Port Kelsey")
			loc = raw_input()
			if loc == "":
				loc = "Port Kelsey"
			print("selected fromdate default 1480447618")
			fd = raw_input()
			if  fd =="":
				fd = 1480447618
			print("selected todate default 1480451001")
			td = raw_input()
			if  td =="":
				td = 1480451001
			
			locationcallsbydatesort(loc,int(fd),int(td))

		# except:
		# 	print("")
		# 	print("****** Problem with the report or report parameters. Please try again...")
		# 	pass