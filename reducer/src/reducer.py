import json
import os

import boto3


def save_results(data, key, bucket, client):
    file = "processing-results/" + key
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
    s3_client = boto3.client("s3")
    results_bucket = os.getenv("RESULTS_BUCKET_URL")

    json_data = json.loads(data)

    save_results(json_data, message_id, results_bucket, s3_client)

    response = sqs_client.delete_message(QueueUrl=master_queue_url, ReceiptHandle=receipt_handle)

    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Message deleted successfully: {message_id}")
