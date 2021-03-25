import json
import os

import boto3


def save_results(data, key, bucket, client):
    file = key
    client.put_object(Bucket=bucket, Key=file, Body=data)


def handler(event, context):
    master_queue_url = os.getenv("REDUCER_QUEUE_URL")
    record = event["Records"][0]
    message_id = record["messageId"]
    receipt_handle = record["receiptHandle"]
    data = record["body"]

    print(data)

    sqs_client = boto3.client("sqs")

    # TODO Write results to s3
    # s3_client = boto3.client("s3")

    # save_results(data, worker_queue_url, sqs_client)

    response = sqs_client.delete_message(QueueUrl=master_queue_url, ReceiptHandle=receipt_handle)

    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Message deleted successfully: {message_id}")
