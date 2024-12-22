from google.cloud import pubsub_v1

# Path to your service account key file
service_account_path = "key.json"

# Define your project ID and subscription ID
project_id = "myownproject241124"
subscription_id = "mysubscript-messagewithschema"

# Create a subscriber client using the service account key
subscriber = pubsub_v1.SubscriberClient.from_service_account_file(service_account_path)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Callback function to process incoming messages
def callback(message):
    try:
        # Decode and process the message
        message_data = message.data.decode("utf-8")
        print(f"Received message: {message_data}")

        # Process attributes if needed
        if message.attributes:
            for key, value in message.attributes.items():
                print(f"Attribute - {key}: {value}")

        # Acknowledge the message to remove it from the queue
        message.ack()
        print("Message acknowledged.")
    except Exception as e:
        print(f"Failed to process message: {e}")
        # Optionally, you can choose not to acknowledge the message



# Start listening to the subscription
print(f"Listening for messages on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)


# Block the main thread while waiting for messages
try:
    streaming_pull_future.result()  # Wait indefinitely
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("Stopped listening for messages.")











