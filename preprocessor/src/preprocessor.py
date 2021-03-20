import os
import csv
import json
import boto3


def chunkit(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def handler(event, context):
    """
    Adds data to the AWS Kinesis Stream to initialize the taxi-data-processing MapReduce system

    :param event: AWS specific event object
    :param context: AWS specific context object
    """

    kinesis_stream_name = os.getenv("KINESIS_STREAM_NAME")  # importdata
    userprofile = os.getenv("userprofile")

    kinesis = boto3.client("kinesis")
    with open(rf"{userprofile}\PycharmProjects\taxi_data_processing\data\sub_set.csv") as f:
        # Creating the ordered Dict
        reader = csv.DictReader(f)
        # putting the json as per the number of chunk we will give in below function
        # Create the list of json and push like a chunk. I am sending 100 rows together
        records = chunkit([{"PartitionKey": 'sau', "Data": json.dumps(row)} for row in reader], 100)
    for chunk in records:
        kinesis.put_records(StreamName=kinesis_stream_name, Records=chunk)
