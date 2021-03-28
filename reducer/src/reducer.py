import json
import os

import boto3
from botocore.exceptions import ClientError


def save_results(data, key, bucket, client):
    client.put_object(Bucket=bucket, Key=key, Body=data)


def handler(event, context):
    master_queue_url = os.getenv("REDUCER_QUEUE_URL")
    record = event["Records"][0]
    message_id = record["messageId"]
    receipt_handle = record["receiptHandle"]
    data = record["body"]

    print(data)

    sqs_client = boto3.client("sqs")

    s3_client = boto3.client("s3")
    results_bucket = os.getenv("RESULTS_BUCKET_URL")

    json_data = json.loads(json.loads(data))

    batch_id = list(json_data.keys())[0]
    json_data = json_data[batch_id]

    print(f"Processing batch: {batch_id}")

    base_bucket_filepath = "processing-results"

    for key in json_data:
        # value_1 = {batch_id: json_data[key]}
        value = json_data[key]
        value = json.dumps(value)

        bucket_folder_path = base_bucket_filepath + "/" + str(key)
        bucket_filepath = bucket_folder_path + "/" + str(batch_id)

        save_results(value, bucket_filepath, results_bucket, s3_client)

    response = sqs_client.delete_message(QueueUrl=master_queue_url, ReceiptHandle=receipt_handle)

    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Message deleted successfully: {message_id}")
