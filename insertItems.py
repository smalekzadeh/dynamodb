import json
import boto3
from datetime import datetime
from collections import defaultdict
import uuid
import time
from faker import Factory


faker = Factory.create()


dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

table = dynamodb.Table("callstab")

itemcount = 0



print ('Insert start time :', time.strftime("%H:%M:%S"))
print int(time.time())


for i in xrange(10):
    accountid = "123" + str(i)
    for a in xrange(10): 
        if a%2==0:
            trunkid="456" + str(i)
        else:
            trunkid="456" + str(i) + str(2)   
        source = "+4414822425" + str(i) + str(a)
        for c in xrange(10000):
            itemcount = itemcount + 1
            callid = str(uuid.uuid4())
            response = table.put_item(
               Item={
                    'callid': callid,
                    'accountid': accountid,
                    'trunkid':trunkid,
                    'source': source,
                    'location': faker.city(),
                    'calldate': int(time.time())

            }
)
print ('Insert end time :', time.strftime("%H:%M:%S"))
print int(time.time())

print ('Insert item count:', itemcount)
