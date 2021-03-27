import os
import csv

import boto3

import json
import time
from random import randint


def get_message_deduplication_id(base_data_id: str) -> str:
    time_nano_epoch = time.time_ns()
    return base_data_id + str(time_nano_epoch)


def send_message(data, queue_url, sqs_client):
    base_data_id = "initial-data-group"
    data_json = json.dumps(data)

    i = randint(1, 1000)
    message_group_id = base_data_id + str(i)

    message_deduplication_id = get_message_deduplication_id(base_data_id)

    sqs_client.send_message(QueueUrl=queue_url,
                            MessageBody=data_json,
                            MessageGroupId=message_group_id,
                            MessageDeduplicationId=message_deduplication_id)


def handler(event, context):
    """
    Adds data to the AWS Kinesis Stream to initialize the taxi-data-processing MapReduce system

    :param event: AWS specific event object
    :param context: AWS specific context object
    """

    # Initialize AWS service clients
    s3 = boto3.client("s3")
    sqs_client = boto3.client("sqs")

    queue_url = os.getenv("INITIAL_DATA_QUEUE")
    bucket_ap = os.getenv("BUCKET_AP")
    dataset_key = os.getenv("DATASET_KEY")

    # Get data csv from AWS S3 storage
    data_object = s3.get_object(Bucket=bucket_ap,
                                Key=dataset_key)
    data = data_object["Body"].read().decode().splitlines()

    # Read csv data and create JSON representation
    reader = csv.DictReader(data)

    # # Purge queue
    # sqs_client.purge_queue(QueueUrl=queue_url)
    # time.sleep(60)

    mini_batch = []
    mini_batch_size = 100
    i = 1
    batch_id = 1

    # Create mini-batches of 100 rows and put to AWS SQS
    print(f"Starting data streaming in mini batches of {mini_batch_size} lines")
    for row in reader:
        mini_batch.append(row)
        if i == mini_batch_size:
            mini_batch_dict = {batch_id: mini_batch}
            send_message(mini_batch_dict, queue_url, sqs_client)
            # print(json.dumps(mini_batch), end="\n\n\n")
            i = 0
            batch_id += 1
            mini_batch.clear()
        i += 1
    else:
        # print(json.dumps(mini_batch), end="\n\n\n")
        mini_batch_dict = {batch_id: mini_batch}
        send_message(mini_batch_dict, queue_url, sqs_client)

    return {"status_code": 200,
            "body": f"Data streaming started in mini batches of {mini_batch_size} lines"}
