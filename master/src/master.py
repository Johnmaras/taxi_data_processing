import base64


def handler(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record["kinesis"]["data"])
        print("Decoded stream: " + str(payload))
