import os
import csv
import json

import boto3


def upload_stream(data, kinesis_stream_name, kinesis_client):
    data_json = json.dumps(data).encode()
    kinesis_client.put_record(StreamName=kinesis_stream_name, Data=data_json, PartitionKey='sau')


def handler(event, context):
    """
    Adds data to the AWS Kinesis Stream to initialize the taxi-data-processing MapReduce system

    :param event: AWS specific event object
    :param context: AWS specific context object
    """

    kinesis_stream_name = os.getenv("KINESIS_STREAM_NAME")  # importdata

    # Initialize AWS service clients
    kinesis = boto3.client("kinesis")
    s3 = boto3.client("s3")

    # Get data csv from AWS S3 storage
    data_object = s3.get_object(Bucket="arn:aws:s3:eu-west-1:820495056858:accesspoint/taxi-data-ap",
                                Key="sub_set_300.csv")
    data = data_object["Body"].read().decode().splitlines()

    # Read csv data and create JSON representation
    reader = csv.DictReader(data)

    mini_batch = []
    i = 1
    batch_id = 1

    # Create mini-batches of 100 rows and put to AWS Kinesis stream
    for row in reader:
        mini_batch.append(row)
        if i == 100:
            upload_stream(mini_batch, kinesis_stream_name, kinesis)
            # print(json.dumps(mini_batch), end="\n\n\n")
            i = 0
            batch_id += 1
            mini_batch.clear()
        i += 1
    else:
        # print(json.dumps(mini_batch), end="\n\n\n")
        upload_stream(mini_batch, kinesis_stream_name, kinesis)
