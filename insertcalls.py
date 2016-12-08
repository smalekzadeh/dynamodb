import json
import boto3
from datetime import datetime
from collections import defaultdict
import uuid
import time
from faker import Factory
import re
from itertools import cycle


faker = Factory.create()


dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

table = dynamodb.Table("callstab3")

itemcount = 0



print ('Insert start time :', time.strftime("%H:%M:%S"))
print int(time.time())

calltype = ["mobile", "nongeo", "inbound", 'nat', "premium","inter", "loc"]
calldirecttion = ["INCOMIN", "OUTGOING"]

calltypeidx = cycle(range(7))
calldirecttionidx = cycle(range(2))

# import pdb; pdb.set_trace()


for i in xrange(2):
    acccount = 0
    accountid = "ACC-123" + str(i)
    for a in xrange(5): 
        if a%2==0:
            trunkid="TRK-X456-" + str(a) + "-" + accountid
        else:
            trunkid="TRK-Y456-" + str(a) + "-" + accountid   

        source = "+4414822425-" + trunkid
        trkcount =0
        numcount =0
        for c in xrange(100):
            itemcount = itemcount + 1
            callid = str(uuid.uuid4())
            response = table.put_item(
               Item={
                    'callid': callid,
                    'accountid': accountid,
                    'trunkid':trunkid,
                    'source': source,
                    'location': re.sub(r'([^\s\w]|_)+', '', faker.country()).replace(' ', '_'),
                    'calltype': calltype[calltypeidx.next()],
                    'calldirection': calldirecttion[calldirecttionidx.next()],
                    'calldate': int(time.time())

                    }
                )        
            acccount += 1
            trkcount += 1
            numcount += 1
        print("trunk ", trunkid, " : ", trkcount )
        print("number ", source, " : ", numcount )
    print("account ", accountid, " : ", acccount )
print ('Insert end time :', time.strftime("%H:%M:%S"))
print int(time.time())

print ('Insert item count:', itemcount)
