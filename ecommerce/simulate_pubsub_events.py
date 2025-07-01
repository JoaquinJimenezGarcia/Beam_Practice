import time
import random
import json
from google.cloud import pubsub_v1
from datetime import datetime
import os

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
TOPIC_ID = "user-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

EVENT_TYPES = ["product_view", "add_to_cart", "checkout", "login", "logout"]
CATEGORIES = ["electronics", "books", "clothing", "toys", "home"]
DEVICES = ["mobile", "desktop", "tablet"]
LOCATIONS = ["Madrid", "Berlin", "Warsaw", "Paris", "Rome"]

def generate_event():
    event = {
        "user_id": random.randint(1000, 2000),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "product_id": random.randint(1000, 2000),
        "category": random.choice(CATEGORIES),
        "device": random.choice(DEVICES),
        "location": random.choice(LOCATIONS)
    }

    if event["event_type"] in ["login", "logout"]:
        del event["product_id"]
        del event["category"]

    return event

def publish_event(event_data):
    data_str = json.dumps(event_data)
    data = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    print(f"Published: {data_str}")
    return future

if __name__ == "__main__":
    while True:
        event = generate_event()
        publish_event(event)
        time.sleep(random.uniform(0.5, 2.0))  # simulate random arrival times
