import os
from pathlib import Path
import tqdm
import boto3

s3_client = boto3.client("s3")

folders = ["Biggest_Route_Quadrant",
           "Longest_Route",
           "Passengers_Per_Vendor",
           "Routes_Per_Quadrant",
           "Routes_With_Spec_Chars"]

bucket = "arn:aws:s3:eu-west-1:820495056858:accesspoint/taxi-data-ap"

objects = s3_client.list_objects(Bucket=bucket)
files = objects["Contents"]
files = list(map(lambda x: x["Key"], files))

progress_bar = tqdm.tqdm(total=len(files))

for folder in folders:
    if not Path(folder).exists():
        os.mkdir(folder)

    folder_files = list(filter(lambda x: x.find(folder) != -1, files))

    for file in folder_files:
        local_file = file.replace("processing-results/", "")
        local_filepath = local_file + ".json"
        s3_client.download_file(Bucket=bucket, Key=file, Filename=local_filepath)
        progress_bar.update()
