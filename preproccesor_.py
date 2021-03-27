import time  
import os
import csv
import json
import time
import boto3
from random import randint

def send_message(data, queue_url, sqs_client):
    data_json = json.dumps(data)
    i = randint(1, 1000)
    message_group_id = "initial-data-group" + str(i)
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=data_json, MessageGroupId=message_group_id)


def handler(event, context):
    """
    Adds data to the AWS SQS to initialize the taxi-data-processing MapReduce system
    :param event: AWS specific event object
    :param context: AWS specific context object
    """

    # Initialize AWS service clients
    start_time = time.time()
    s3 = boto3.client("s3")
    sqs_client = boto3.client("sqs")

    queue_url = 'https://sqs.us-east-2.amazonaws.com/957241440788/mastersqs.fifo'
    print(queue_url)
    # Get data csv from AWS S3 storage
    data_object = s3.get_object(Bucket="taxi-data-ap",
                                Key="sub_set.csv")
    data = data_object["Body"].read().decode().splitlines()

    # Read csv data and create JSON representation
    reader = csv.DictReader(data)

    mini_batch = []
    mini_batch_size = 100
    i = 1
    batch_id = 1

    # Create mini-batches of 100 rows and put to AWS SQS
    print(f"Starting data streaming in mini batches of {mini_batch_size} lines")
    for row in reader:
        mini_batch.append(row)
        if i == mini_batch_size:
            send_message(mini_batch, queue_url, sqs_client)
            time.sleep(4)
            print(json.dumps(mini_batch))
            i = 0
            batch_id += 1
            mini_batch.clear()
        i += 1
    else:
        print(json.dumps(mini_batch))
        send_message(mini_batch, queue_url, sqs_client)
        time.sleep(4)

    end_time = (time.time()-start_time)
    print("Total time for processing data: ", end_time)
    return {"status_code": 200,
            "body": f"Data streaming started in mini batches of {mini_batch_size} lines"}