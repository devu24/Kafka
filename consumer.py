from pymongo.mongo_client import MongoClient
from confluent_kafka import Consumer  # Correct import
import json
import time

conf = {
    'bootstrap.servers': 'pkc-w77k7w.centralus.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '2RDOO2YEL2YAMK3G',
    'sasl.password': 'cfltU7EJnz9a5Wg0Q8ASWj56PV0U+I9U/YmPmaLCIz6biXv4l+ZNI1/n3A5p3xCg',
    'group.id': 'python_example_group_1',
    'auto.offset.reset': 'earliest'
}

uri = "mongodb+srv://Devendra:devendra@devendra.e3kfvth.mongodb.net/?retryWrites=true&w=majority&appName=Devendra"
client = MongoClient(uri)
db = client['kafka_db']
coll = db['kafka_messages']

consumer = Consumer(conf)
topic = "devu_topic1"

consumer.subscribe([topic])
print(f"Subscribed to topic: {topic} and listining the messages")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue
        data = json.loads(msg.value().decode('utf-8'))

        transformed_data = {
            "index": data.get("index", 0),
            "new messages": data.get("new messages"),
            "timestamp": data.get("timestamp"),
            "source": "Hi from Kafka"
        }

        coll.insert_one(transformed_data)  # Insert into MongoDB.
        print(f"Inserted message into MongoDB: {transformed_data}")
        print(f"Consumed message: {data}")

except Exception as e:  # Correct exception class
    print(f"Error consuming messages: {e}")

finally:
    consumer.close()
    MongoClient.close()
    print("Consumer closed and MongoDB connection closed.")
