from google.cloud import pubsub_v1

PROJECT_ID = "ds-561-mohitsai"
SUBSCRIPTION_ID = "banned-country-topic-sub"

def callback(message):
    print(f"Received banned country request: {message.data.decode('utf-8')}")
    message.ack()

def listen_for_banned_requests():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    
    future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()

if __name__ == "__main__":
    listen_for_banned_requests()
