from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('orders', 
                         bootstrap_servers='localhost:9092', 
                         auto_offset_reset='earliest',
                         consumer_timeout_ms=100)

while True:
    messages = [msg.value.decode('utf-8') for msg in consumer]
    for msg in messages:
        print(msg)
    if input('>>>') == 'q' : break