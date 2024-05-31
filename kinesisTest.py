import boto3
import base64
import time

# Initialize the Kinesis client
kinesis_client = boto3.client('kinesis', region_name='us-east-2')

# Data to be sent to the stream
data = "Hello world"

# Put record into the Kinesis stream
response = kinesis_client.put_record(
    StreamName='first_stream',  # Your Kinesis stream name
    Data=base64.b64encode(data.encode('utf-8')),  # Encode data to base64
    PartitionKey='testKey'  # Partition key
)

print("Put record response:", response)

# Waits for the Lambda process the record
time.sleep(5)



# If you want to check CloudWatch logs via script (optional)
cloudwatch_client = boto3.client('logs', region_name='us-east-2')

log_group_name = '/aws/lambda/processKinesisData'
log_streams = cloudwatch_client.describe_log_streams(
    logGroupName=log_group_name,
    orderBy='LastEventTime',
    descending=True
)

if log_streams['logStreams']:
    latest_log_stream = log_streams['logStreams'][0]['logStreamName']
    log_events = cloudwatch_client.get_log_events(
        logGroupName=log_group_name,
        logStreamName=latest_log_stream,
        startFromHead=True
    )

    for event in log_events['events']:
        print(event['message'])
else:
    print("No log streams found.")