from kafka import KafkaConsumer

consumer = KafkaConsumer('test_kafka_frukter',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset = 'earliest',
                         consumer_timeout_ms=100)

while True:
    messages = [msg.value.decode('utf-8') for msg in consumer]
    for message in messages:
        print(message)
    if input('Skriv i my_app_topic2:') == 'q': break
    