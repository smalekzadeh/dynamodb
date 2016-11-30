import boto3
from boto3.dynamodb.conditions import Key
import time

dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

table = dynamodb.Table("calls")

# print ('Query a source calls start time :', time.strftime("%H:%M:%S"))

# response = table.query(
#     IndexName='source-calldate-index',
#     KeyConditionExpression=Key('source').eq("+441482242500")
# )

# print ('Query  a source calls end time :', time.strftime("%H:%M:%S"))

# print "Returned item count: "
# print len(response['Items'])



# print ('Query a source and date sort range calls end time :', time.strftime("%H:%M:%S"))


# response = table.query(
#     IndexName='source-calldate-index',
#     KeyConditionExpression=Key('source').eq("+441482242500") & Key('calldate').between('A', 'L')
# )

# print ('Query a source and date sort range calls end time :', time.strftime("%H:%M:%S"))

# print "Returned item count: "
# print len(response['Items'])



response = table.delete()
import pdb; pdb.set_trace()

