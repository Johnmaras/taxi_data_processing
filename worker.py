import json
import os
from random import randint
from typing import Tuple
import time
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

def get_lat(latitude: Tuple):
    lat = float(latitude[0])
    
    return lat
    
def get_lon(longitude: Tuple):
    lon = float(longitude[0])
    
    return lon
    
def get_vendor(vid: Tuple):
    vendor_id = int(vid[0])
    
    return vendor_id    

def process_data(data: json):
    key_1 = 1
    key_2 = 2
    key_3 = 3

    vendor_id_1 = []
    vendor_id_2 = []
    area_1 = []
    area_2 = []
    area_3 = []
    area_4 = []
    key_2_results = []
    key_3_results = []

    # print(type(data))

    for record in data:
        # Query 1:
        cent_lat = float(40.76793672)
        cent_lon = float(-73.98215480)
        
        lat_temp = (record["pickup_latitude"], record["dropoff_latitude"])
        lon_temp = (record["pickup_longitude"], record["dropoff_longitude"])
        
        lat = get_lat(lat_temp)
        lon = get_lon(lon_temp)
        
        if lat > cent_lat and lon < cent_lon:
            area_1.append(record)
        elif lat > cent_lat and lon > cent_lon:
            area_2.append(record)
        elif lat < cent_lat and lon < cent_lon:
            area_3.append(record)
        elif lat < cent_lat and lon > cent_lon:
            area_4.append(record)
            
        # Query 3:
        vid = (record["vendor_id"])
        vendor_id = get_vendor(vid) 
        
        if vendor_id == 1:
            vendor_id_1.append(record)
        elif vendor_id ==2:
            vendor_id_2.append(record)
            
		# Query 2:
        # Get values
        latitude = (record["pickup_latitude"], record["dropoff_latitude"])
        longitude = (record["pickup_longitude"], record["dropoff_longitude"])

        l_R = get_distance(latitude, longitude)
        t_R = int(record["trip_duration"]) / 60
        p_R = int(record["passenger_count"])

        if l_R > 1000 and t_R > 10 and p_R > 2:
            key_2_results.append(record)
    
    a1 = len(area_1)
    a2 = len(area_2)
    a3 = len(area_3)
    a4 = len(area_4)
    vid1 = len(vendor_id_1)
    vid2 = len(vendor_id_2)
    
    results = {key_1: " ", "Area:1": a1,"Area:2": a2, "Area3:": a3, "Area4:":a4, key_2: key_2_results, key_3: " ","Vendor 1:":vid1, "Vendor 2:": vid2}

    return results


def send_message(data, queue_url, sqs_client):
    data_json = json.dumps(data)
    i = randint(1, 1000)
    message_group_id = "reducer-data-group" + str(i)
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=data_json, MessageGroupId=message_group_id)


def handler(event, context):
    start_time = time.time()
    worker_queue_url = 'https://sqs.us-east-2.amazonaws.com/957241440788/workersqs.fifo'
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
    reducer_queue_url = 'https://sqs.us-east-2.amazonaws.com/957241440788/reducersqs.fifo'
    results_json = json.dumps(results)
    send_message(results_json, reducer_queue_url, sqs_client)

    #sqs_client = boto3.client("sqs")
    response = sqs_client.delete_message(QueueUrl=worker_queue_url, ReceiptHandle=receipt_handle)

    if response and response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"Message deleted successfully: {message_id}")

    end_time = (time.time()-start_time)
    print("Total time for worker job : ", end_time)