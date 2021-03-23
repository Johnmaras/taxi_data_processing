import csv
import json
import boto3

def send_sqs_message(QueueName, msg_body):
# Send the SQS message
    sqs_client = boto3.client('sqs')
    sqs_queue_url = sqs_client.get_queue_url(
    QueueName=QueueName
)['QueueUrl']
    try:
        msg = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                      MessageBody=json.dumps(msg_body))
    except ClientError as e:
        logging.error(e)
        return None
    return msg

def handler(event, context):
    """
    Adds data to the AWS Kinesis Stream to initialize the taxi-data-processing MapReduce system
    :param event: AWS specific event object
    :param context: AWS specific context object
    """

    QueueName = 'firstsqs' #sqs name 
    # Initialize AWS service clients
    s3 = boto3.client("s3")

    # Get data csv from AWS S3 storage
    data_object = s3.get_object(Bucket="taxi-data-ap",
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
        if i == 10:
            msg = send_sqs_message(QueueName,mini_batch)
            print(json.dumps(mini_batch))
            i = 0
            batch_id += 1
            mini_batch.clear()
        i += 1
    else:
         msg = send_sqs_message(QueueName,mini_batch)
         print(json.dumps(mini_batch))
   
  