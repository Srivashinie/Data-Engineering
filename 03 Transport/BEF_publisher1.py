import json
import time
from google.cloud import pubsub_v1

# TODO(developer)
project_id = "data-engineering-420102"
topic_id = "gpubsub"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

ct=0

with open('bcsample.json', 'r') as file:
    data = json.load(file)

for item in data:
    data_str = json.dumps(item)
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    ct=ct+1
    time.sleep(0.25)
    #print(future.result())
print(f"The total number of messages sent:{ct}")