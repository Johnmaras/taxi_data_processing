import json
import boto3


def get_messages_from_queue(sqs_client, queue_url):
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to read.

    See https://alexwlchan.net/2018/01/downloading-sqs-queues/

    """
    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url, AttributeNames=["All"], MaxNumberOfMessages=10
        )

        try:
            yield from resp["Messages"]
        except KeyError:
            return

        entries = [
            {"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]}
            for msg in resp["Messages"]
        ]

        resp = sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)

        if len(resp["Successful"]) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )

   
def reducer(event, context):
    
    s3 = boto3.client('s3')
    sqs_client = boto3.client('sqs')
    bucket ='taxi-data-ap'
        
    dst_queue_url ="https://sqs.us-east-2.amazonaws.com/957241440788/secsqs"
    for message in get_messages_from_queue(sqs_client, queue_url=dst_queue_url):
        uploadByteStream = bytes(json.dumps(message).encode('UTF-8'))
        s3.put_object(Bucket=bucket, Key="results.txt", Body=uploadByteStream)
        print(message["Body"])    
