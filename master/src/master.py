import json
import os
import time
from random import randint

import boto3


def get_message_deduplication_id(base_data_id: str) -> str:
    time_nano_epoch = time.time_ns()
    return base_data_id + str(time_nano_epoch)


def send_message(data, queue_url, sqs_client):
    base_data_id = "worker-data-group"
    data_json = json.dumps(data)

    i = randint(1, 1000)
    message_group_id = base_data_id + str(i)

    message_deduplication_id = get_message_deduplication_id(base_data_id)

    sqs_client.send_message(QueueUrl=queue_url,
                            MessageBody=data_json,
                            MessageGroupId=message_group_id,
                            MessageDeduplicationId=message_deduplication_id)


def handler(event, context):
    master_queue_url = os.getenv("MASTER_QUEUE_URL")
    record = event["Records"][0]
    message_id = record["messageId"]
    receipt_handle = record["receiptHandle"]
    data = record["body"]

    print(f"Sending message {message_id} to workers")

    sqs_client = boto3.client("sqs")

    worker_queue_url = os.getenv("WORKER_QUEUE_URL")
    send_message(data, worker_queue_url, sqs_client)

    response = sqs_client.delete_message(QueueUrl=master_queue_url, ReceiptHandle=receipt_handle)

    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Message deleted successfully: {message_id}")
