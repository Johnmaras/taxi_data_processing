import json
import os

import boto3


def send_message(data, queue_url, sqs_client):
    data_json = json.dumps(data)
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=data_json, MessageGroupId="worker-data-group")


def handler(event, context):
    master_queue_url = 'https://sqs.us-east-2.amazonaws.com/957241440788/firstsqs.fifo'
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