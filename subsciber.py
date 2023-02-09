"""
This script read the notification messages and copies file from staging to curated buckets.
"""

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import storage
import argparse
# project_id = "your-project-id"
# subscription_id = "your-subscription-id"
# Number of seconds the subscriber should listen for messages
timeout = 5.0

subscriber = pubsub_v1.SubscriberClient()


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message.data}.")
    if message.attributes:
        print("Attributes:")
        for key in message.attributes:
            value = message.attributes.get(key)
            print(f"{key}: {value}")
        bucket_name = message.attributes.get('bucketId')
        blob_name = message.attributes.get('objectId')
        # ex. 'data/some_location/file_name'
        # Logic to change bucket name from bucket10 to buckets
        new_bucket_name = bucket_name.replace("bucket10", "buckets")
        new_blob_name = blob_name
        # ex. 'data/destination/file_name'

        storage_client = storage.Client()
        source_bucket = storage_client.get_bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.get_bucket(new_bucket_name)

        # copy to new destination
        if source_blob.exists(storage_client):
            source_bucket.copy_blob(
                source_blob, destination_bucket, new_blob_name)
            # delete in old destination
            source_blob.delete()

            print(f'File moved from {source_blob} to {destination_bucket} {new_blob_name}')
    message.ack()



# Wrap subscriber in a 'with' block to automatically call close() when done.
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project_id', required=True,
        help='Project Id where pubsub is configured')
    parser.add_argument(
        '--subscription_id', required=True,
        help='Subscription id for topic')
    args = parser.parse_args()
    project_id = args.project_id
    subscription_id = args.subscription_id
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            # streaming_pull_future.result(timeout=timeout)
            #streaming_pull_future.result(timeout=timeout)
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.