import json
import os
import zlib
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1, storage
from datetime import datetime
import rsa
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from base64 import b64encode, b64decode


#Generate RSA keys
(public_key, private_key) = rsa.newkeys(2048)

# TODO(developer)
project_id = "data-engineering-420102"
subscription_id = "archivetest-sub"
bucket_name = "archbuckets"
data = []
ct = 0

# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

#Create a Storage client
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"archivecompressencrypt_{today}.json.gz"
blob = bucket.blob(file_name)

#Function to encrypt data using RSA
def encrypt_data(data: bytes, public_key):
    #Generate random key
    aes_key = get_random_bytes(32)  

    #Encrypt data 
    cipher_aes = AES.new(aes_key, AES.MODE_GCM)
    ciphertext, tag = cipher_aes.encrypt_and_digest(data)

    #Encrypt the AES key with RSA
    encrypted_aes_key = rsa.encrypt(aes_key, public_key)

    #Encode the message
    encrypted_data = {
        'ciphertext': b64encode(ciphertext).decode('utf-8'),
        'tag': b64encode(tag).decode('utf-8'),
        'nonce': b64encode(cipher_aes.nonce).decode('utf-8'),
        'aes_key': b64encode(encrypted_aes_key).decode('utf-8')
    }

    return json.dumps(encrypted_data).encode('utf-8')

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
            compressed_data = zlib.compress('\n'.join(data).encode('utf-8'))
            encrypted_data = encrypt_data(compressed_data, public_key)
            blob.upload_from_string(encrypted_data, content_type='application/octet-stream')
            print(f"Uploaded encrypted compressed messages to {bucket_name}/{file_name}")
        streaming_pull_future.cancel()

