from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# TODO: Set your project ID and topic ID
project_id = "data-engineering-420102"
topic_id = "gpubsub"
subscription_id = "my-sub"
timeout = 5.0

subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(project_id, topic_id)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    # Acknowledge the message without processing it
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..")


with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result() 
