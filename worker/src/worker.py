import json
import os
from random import randint
from typing import Tuple

import boto3
import math


def get_distance(latitude: Tuple, longitude: Tuple):
    lat1 = float(latitude[0])
    lat2 = float(latitude[1])
    lon1 = float(longitude[0])
    lon2 = float(longitude[1])

    earth_radius = 6371e3  # Earth radius in metres
    f1 = lat1 * math.pi / 180  # φ, λ in radians
    f2 = lat2 * math.pi / 180
    Df = (lat2 - lat1) * math.pi / 180
    Dl = (lon2 - lon1) * math.pi / 180
    a = math.sin(Df / 2) * math.sin(Df / 2) + math.cos(f1) * math.cos(f2) * math.sin(Dl / 2) * math.sin(Dl / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = earth_radius * c  # in metres

    return d


def process_data(data: json):
    key_1 = 1
    key_2 = 2

    key_1_results = []
    key_2_results = []

    # print(type(data))

    for record in data:
        # TODO key_1

        # print("Record" + str(record))

        # Get values
        latitude = (record["pickup_latitude"], record["dropoff_latitude"])
        longitude = (record["pickup_longitude"], record["dropoff_longitude"])

        l_R = get_distance(latitude, longitude)
        t_R = int(record["trip_duration"]) / 60
        p_R = int(record["passenger_count"])

        if l_R > 1000 and t_R > 10 and p_R > 2:
            key_2_results.append(record)

    results = {key_1: key_1_results, key_2: key_2_results}

    return results


def send_message(data, queue_url, sqs_client):
    data_json = json.dumps(data)
    i = randint(1, 1000)
    message_group_id = "reducer-data-group" + str(i)
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=data_json, MessageGroupId=message_group_id)


def handler(event, context):
    worker_queue_url = os.getenv("WORKER_QUEUE_URL")
    record = event["Records"][0]
    message_id = record["messageId"]
    receipt_handle = record["receiptHandle"]
    data = record["body"]

    # print(f"Got a message: messageID = {message_id}\n"
    #       f"receiptHandle = {receipt_handle}\n"
    #       f"body = {data}")

    # json_data = json.loads(data)
    json_data = json.loads(json.loads(data))
    # print(json_data)
    results = process_data(json_data)

    # print(results)

    sqs_client = boto3.client("sqs")

    # Send results to reducer queue
    reducer_queue_url = os.getenv("REDUCER_QUEUE_URL")
    results_json = json.dumps(results)
    send_message(results_json, reducer_queue_url, sqs_client)

    sqs_client = boto3.client("sqs")
    response = sqs_client.delete_message(QueueUrl=worker_queue_url, ReceiptHandle=receipt_handle)

    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Message deleted successfully: {message_id}")
