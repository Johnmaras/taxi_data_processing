import json
import os
import boto3
import time
from random import randint

def save_results(data, key, bucket, client):
    file = key
    client.put_object(Bucket=bucket, Key=file, Body=data)


def handler(event, context):
    start_time = time.time()
    reducer_queue_url = 'https://sqs.us-east-2.amazonaws.com/957241440788/reducersqs.fifo'
    record = event["Records"][0]
    message_id = record["messageId"]
    receipt_handle = record["receiptHandle"]
    data = record["body"]

    print(data)

    #sqs_client = boto3.client("sqs")

    # TODO Write results to s3
    # s3_client = boto3.client("s3")

    # save_results(data, worker_queue_url, sqs_client)
    
    s3 = boto3.client('s3')
    bucket ='taxi-data-ap'
    i = randint(1, 1000)
    print(f"Uploading file " + str(i) + " to s3 bucket...")
    uploadByteStream = bytes(json.dumps(data).encode('UTF-8'))
    fileName ='result_of_batch_' + str(i) + '.txt'
    s3.put_object(Bucket=bucket, Key=fileName, Body=uploadByteStream)
    i += 1
    sqs_client = boto3.client("sqs")

    response = sqs_client.delete_message(QueueUrl=reducer_queue_url, ReceiptHandle=receipt_handle)

    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Message deleted successfully: {message_id}")
        
    end_time = (time.time()-start_time)
    print("Total time for reducer job : ", end_time)