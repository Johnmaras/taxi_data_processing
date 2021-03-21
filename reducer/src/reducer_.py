import json
import boto3

s3 = boto3.client('s3')
def reducer(event,context):

	bucket ='taxi-data-ap'
	fileName ='results'+ '.json'
	uploadByteStream = bytes(json.dumps(event).encode('UTF-8'))
	
	# put results of data into the s3 bucket
    
	s3.put_object(Bucket=bucket, Key=fileName, Body=uploadByteStream)

	print(uploadByteStream)
	print('Put Complete')
	