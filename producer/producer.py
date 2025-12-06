from confluent_kafka import Producer
import socket
import json
import time
import os


bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
topic = os.getenv('KAFKA_TOPIC', 'mock_data')
conf = {"bootstrap.servers": bootstrap_servers,
        "client.id": socket.gethostname()}

producer = Producer(conf)

with open("data.json", encoding="UTF-8") as file_in:
    records = json.load(file_in)

for i in range(len(records)):
    value = json.dumps(records[i]).encode("UTF-8")
    producer.produce(topic=topic,
                     key=str(i).encode("UTF-8"), 
                     value=value)
    producer.poll(0)
producer.flush()

