from confluent_kafka import Producer
import json
import time

conf = {
    'bootstrap.servers': 'pkc-w77k7w.centralus.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',  # Fixed typo here
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '2RDOO2YEL2YAMK3G',
    'sasl.password': 'cfltU7EJnz9a5Wg0Q8ASWj56PV0U+I9U/YmPmaLCIz6biXv4l+ZNI1/n3A5p3xCg'  # Ensure this is your actual API secret
}

producer = Producer(conf)  # Fixed variable name

topic = "devu_topic1"

for i in range(100):
    data = {"index": i, "new messages": f"Hello, Kafka! Message {i}", 'timestamp': time.time()}
    producer.produce(topic, key=str(i), value=json.dumps(data))
    print(f"Produced message {i}: {data}")
    time.sleep(1)

producer.flush()
print("All messages produced successfully.")