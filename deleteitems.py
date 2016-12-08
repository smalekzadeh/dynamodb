import boto3
from boto3.dynamodb.conditions import Key
import time
import datetime
from collections import defaultdict
from collections import Counter

dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

table = dynamodb.Table("calls")

#  query a index
print ('Query a trunk calls start time :', time.strftime("%H:%M:%S"))

response = table.scan()

for record in response["Items"]:  
	table.delete_item(
				    Key={
				        'callid': record["callid"]
				    	}
					)

# import pdb;pdb.set_trace()

print ('Query  a trunk calls end time   :', time.strftime("%H:%M:%S"), '(getting first 1M which is ', response['Count'], 'the limit') 

# use the below code to scan the whole results and get the count
rowcount = response['Count']

while 'LastEvaluatedKey' in response:

	response = table.scan()
	for record in response["Items"]:  
		table.delete_item(
					    Key={
					        'callid': record["callid"]
					    }
					)

	rowcount = response['Count'] + rowcount

    # 

print ("Query results item count: ", rowcount)