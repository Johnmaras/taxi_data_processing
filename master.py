import json
import os
import boto3
import time
from random import randint

def send_message(data, queue_url, sqs_client):
    data_json = json.dumps(data)
    i = randint(1, 1000)
    message_group_id = "initial-data-group" + str(i)
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=data_json, MessageGroupId=message_group_id)

def handler(event, context):
    start_time = time.time()
    master_queue_url = 'https://sqs.us-east-2.amazonaws.com/957241440788/mastersqs.fifo'
    record = event["Records"][0]
    message_id = record["messageId"]
    receipt_handle = record["receiptHandle"]
    data = record["body"]

    print(f"Sending message {message_id} to workers")

    sqs_client = boto3.client("sqs")

    worker_queue_url = 'https://sqs.us-east-2.amazonaws.com/957241440788/workersqs.fifo'
    send_message(data, worker_queue_url, sqs_client)

    response = sqs_client.delete_message(QueueUrl=master_queue_url, ReceiptHandle=receipt_handle)

    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Message deleted successfully: {message_id}")
    
    end_time = (time.time()-start_time)
    print("Total time for master job : ", end_time)