import os

import boto3


def handler(event, context):
    queue_url = os.getenv("WORKER_QUEUE_URL")
    record = event["Records"][0]
    message_id = record["messageId"]
    receipt_handle = record["receiptHandle"]
    data = record["body"]

    print(f"Got a message: messageID = {message_id}\n"
          f"receiptHandle = {receipt_handle}\n"
          f"body = {data}")

    sqs_client = boto3.client("sqs")
    response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Message deleted successfully: {message_id}")
