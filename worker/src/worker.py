import json
import os
import time
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


def get_quadrant(latitude: Tuple, longitude: Tuple) -> str:
    ny_lat = 40.76793672
    ny_lon = -73.98215480

    lat = latitude[0]
    lon = longitude[0]

    if lat > ny_lat and lon < ny_lon:
        return "Area 1"
    elif lat > ny_lat and lon > ny_lon:
        return "Area 2"
    elif lat < ny_lat and lon < ny_lon:
        return "Area 3"
    elif lat < ny_lat and lon > ny_lon:
        return "Area 4"

    return ""


def process_data(data: json):
    # Query 1 init
    key_1 = "Routes_Per_Quadrant"
    areas = {"Area 1": 0, "Area 2": 0, "Area 3": 0, "Area 4": 0}
    key_1_results = []
    # End Query 1 init

    # Query 2 init
    key_2 = "Routes_With_Spec_Chars"
    key_2_results = []
    # End Query 2 init

    # Query 3 init
    key_3 = "Biggest_Route_Quadrant"
    max_duration = 0
    max_duration_area = ""
    # End Query 3 init

    # Query 4 init
    key_4 = "Longest_Route"
    # End Query 4 init

    # Query 5 init
    key_5 = "Passengers_Per_Vendor"
    vendors_passengers = {}
    key_5_results = []
    # End Query 5 init

    # print(type(data))

    batch_id = list(data.keys())[0]
    print(f"Processing batch: {batch_id}")
    data = data[batch_id]

    longest_route_len = 0
    longest_route_record = {}
    for record in data:
        # print("Record" + str(record))

        # Get values
        latitude = (float(record["pickup_latitude"]), float(record["dropoff_latitude"]))
        longitude = (float(record["pickup_longitude"]), float(record["dropoff_longitude"]))

        # Query 1
        quadrant = get_quadrant(latitude, longitude)
        area_count = areas.get(quadrant,
                               0)  # In case something wrong happend and no valid quadrant was returned by get_quadrant()
        area_count += 1
        areas[quadrant] = area_count
        # End Query 1

        # Query 2
        l_R = get_distance(latitude, longitude)
        trip_duration = int(record["trip_duration"])
        t_R = trip_duration / 60
        p_R = int(record["passenger_count"])

        if l_R > 1000 and t_R > 10 and p_R > 2:
            key_2_results.append(record)
        # End Query 2

        # Query 3
        if max_duration < trip_duration:
            max_duration = trip_duration
            max_duration_area = quadrant
        # End Query 3

        # Query 4
        if longest_route_len < l_R:
            longest_route_len = l_R
            longest_route_record = record
        # End Query 4

        # Query 5
        vendor_id = record["vendor_id"]
        passengers = int(record["passenger_count"])
        num_of_passengers = vendors_passengers.get(vendor_id, 0)
        num_of_passengers += passengers
        vendors_passengers[vendor_id] = num_of_passengers
        # End Query 5

    key_1_results = areas
    key_3_results = {"Area": max_duration_area, "Route Time(secs)": max_duration}
    key_4_results = {"Record": longest_route_record, "Route Length": longest_route_len}
    for vendor_id in vendors_passengers:
        passengers = vendors_passengers[vendor_id]
        results_record = {"Vendor ID": vendor_id, "Passenger": passengers}
        key_5_results.append(results_record)

    results = {batch_id: {key_1: key_1_results,
                          key_2: key_2_results,
                          key_3: key_3_results,
                          key_4: key_4_results,
                          key_5: key_5_results}}

    return results


def get_message_deduplication_id(base_data_id: str) -> str:
    time_nano_epoch = time.time_ns()
    return base_data_id + str(time_nano_epoch)


def send_message(data, queue_url, sqs_client):
    base_data_id = "reducer-data-group"
    data_json = json.dumps(data)

    i = randint(1, 1000)
    message_group_id = base_data_id + str(i)

    message_deduplication_id = get_message_deduplication_id(base_data_id)

    sqs_client.send_message(QueueUrl=queue_url,
                            MessageBody=data_json,
                            MessageGroupId=message_group_id,
                            MessageDeduplicationId=message_deduplication_id)


def handler(event, context):
    worker_queue_url = os.getenv("WORKER_QUEUE_URL")
    record = event["Records"][0]
    message_id = record["messageId"]
    receipt_handle = record["receiptHandle"]
    data = record["body"]

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
