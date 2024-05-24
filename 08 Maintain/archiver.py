import json
import os
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1, storage
from datetime import datetime

# TODO(developer)
project_id = "data-engineering-420102"
subscription_id = "archivetest-sub"
bucket_name = "archbuckets"
# Number of seconds the subscriber should listen for messages
data=[]
ct = 0
subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"archive_{today}.json"
blob = bucket.blob(file_name)
#Open a file to write the messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = message.data.decode('utf-8')
    data.append(json_message)
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        if data:
           blob.upload_from_string('\n'.join(data), content_type='application/json')
           print(f"Uploaded messages to {bucket_name}/{file_name}")
        streaming_pull_future.cancel()
        #streaming_pull_future.result()
