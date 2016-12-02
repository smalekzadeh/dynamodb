import boto3
from boto3.dynamodb.conditions import Key
import time

dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

table = dynamodb.Table("calls")

response = table.delete()
import pdb; pdb.set_trace()

