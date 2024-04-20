from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# TODO: Set your project ID
project_id = "data-engineering-420102"
subscription_id = "my-sub1"
file_name = "data2.txt"

def receive_messages(subscription_id, file_name):
    # Number of seconds the subscriber should listen for messages
    timeout = 35.0

    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    with open(file_name, "w") as file:
        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            file.write(f"{message.data.decode('utf-8')}\n")
            message.ack()

        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        print(f"Listening for messages on {subscription_path}..\n")

        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()  # Trigger the shutdown.
                streaming_pull_future.result()  # Block until the shutdown is complete.

if __name__ == "__main__":
    receive_messages(subscription_id, file_name)