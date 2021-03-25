import os
import json
import boto3


def handler(event, context):
    queue_url = 'https://sqs.us-east-2.amazonaws.com/957241440788/workersqs.fifo'
    record = event["Records"][0]
    message_id = record["messageId"]
    receipt_handle = record["receiptHandle"]
    data = record["body"]

    print(f"Got a message: messageID = {message_id}\n"
          f"receiptHandle = {receipt_handle}\n"
          f"body = {data}")
          
    s3 = boto3.client('s3')
    bucket ='taxi-data-ap'
    i=1
    print(f"Uploading file " + str(i) + " to s3 bucket...")
    uploadByteStream = bytes(json.dumps(data).encode('UTF-8'))
    fileName ='result_of_batch_' + str(i) + '.txt'
    s3.put_object(Bucket=bucket, Key=fileName, Body=uploadByteStream)
    i += 1
    sqs_client = boto3.client("sqs")
    response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Message deleted successfully: {message_id}")