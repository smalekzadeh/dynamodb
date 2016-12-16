import json
import boto3
from datetime import datetime
from collections import defaultdict
import uuid
import time
from faker import Factory
import re
from itertools import cycle
import calendar


faker = Factory.create()

print ('Insert start time :', time.strftime("%H:%M:%S"))

def insertstats(item):
    hour_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%Y-%m-%dT%H"),"%Y-%m-%dT%H").utctimetuple()) # datetime.now().strftime("%Y-%m-%dT%H")
    day_event_time=  calendar.timegm(datetime.strptime(datetime.now().strftime("%Y-%m-%d"),"%Y-%m-%d").utctimetuple())  #datetime.now().strftime("%Y-%m-%d")
    month_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%Y-%m"),"%Y-%m").utctimetuple())   #datetime.now().strftime("%Y-%m")
    year_event_time= calendar.timegm(datetime.strptime(datetime.now().strftime("%Y"),"%Y").utctimetuple())#datetime.now().strftime("%Y")

    update_counter( item["accountid"], item["location"],item["calltype"],hour_event_time, 1)
    update_counter( item["accountid"], item["location"],item["calltype"],day_event_time, 1)
    update_counter( item["accountid"], item["location"],item["calltype"],month_event_time, 1)
    update_counter( item["accountid"], item["location"],item["calltype"],year_event_time, 1)

    update_counter( item["trunkid"], item["location"],item["calltype"],hour_event_time, 1)
    update_counter( item["trunkid"], item["location"],item["calltype"],day_event_time, 1)
    update_counter( item["trunkid"], item["location"],item["calltype"],month_event_time, 1)
    update_counter( item["trunkid"], item["location"],item["calltype"],year_event_time, 1)

    update_counter( item["source"], item["location"],item["calltype"],hour_event_time, 1)
    update_counter( item["source"], item["location"],item["calltype"],day_event_time, 1)
    update_counter( item["source"], item["location"],item["calltype"],month_event_time, 1)
    update_counter( item["source"], item["location"],item["calltype"],year_event_time, 1)


# update the account  call counts
def update_counter(entity, location, calltype, event_datetime, event_count=1, dynamodb = boto3.resource(service_name='dynamodb', region_name='eu-west-1')):
    table = dynamodb.Table('call_counts')
    
    
    
    # insert the item
    response = table.update_item(
    Key={
        'entityid': entity, 
        'date_sort': event_datetime
    },
    ExpressionAttributeValues={":value":event_count},
    UpdateExpression="ADD AA_TOTALS_COUNT :value, %s :value , %s :value" % (location, calltype)
    )

    pass




dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

table = dynamodb.Table("calls")

itemcount = 0

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

            item={
                    'callid': callid,
                    'accountid': accountid,
                    'trunkid':trunkid,
                    'source': source,
                    'location': re.sub(r'([^\s\w]|_)+', '', faker.country()).replace(' ', '_'),
                    'calltype': calltype[calltypeidx.next()],
                    'calldirection': calldirecttion[calldirecttionidx.next()],
                    'calldate': int(datetime.now().strftime("%Y%m%d%H%M%S"))

                    }
            response = table.put_item(Item = item)        
            acccount += 1
            trkcount += 1
            numcount += 1
            insertstats(item)
        print("trunk ", trunkid, " : ", trkcount )
        print("number ", source, " : ", numcount )
    print("account ", accountid, " : ", acccount )
print ('Insert end time :', time.strftime("%H:%M:%S"))

print ('Insert item count:', itemcount)
