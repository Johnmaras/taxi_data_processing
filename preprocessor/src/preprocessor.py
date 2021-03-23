import os
import csv
import json

import boto3


def send_message(data, queue_url, sqs_client):
    data_json = json.dumps(data)
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=data_json, MessageGroupId="initial-data-group")


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

    # Get data csv from AWS S3 storage
    data_object = s3.get_object(Bucket="arn:aws:s3:eu-west-1:820495056858:accesspoint/taxi-data-ap",
                                Key="sub_set_300.csv")
    data = data_object["Body"].read().decode().splitlines()

    # Read csv data and create JSON representation
    reader = csv.DictReader(data)

    mini_batch = []
    mini_batch_size = 100
    i = 1
    batch_id = 1

    # Create mini-batches of 100 rows and put to AWS Kinesis stream
    for row in reader:
        mini_batch.append(row)
        if i == mini_batch_size:
            send_message(mini_batch, queue_url,sqs_client)
            # print(json.dumps(mini_batch), end="\n\n\n")
            i = 0
            batch_id += 1
            mini_batch.clear()
        i += 1
    else:
        # print(json.dumps(mini_batch), end="\n\n\n")
        send_message(mini_batch, queue_url, sqs_client)
