from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

while(True):
    msg = input('Skriv något:')
    if msg == "q" : 
        break
    producer.send('test_kafka_frukter', bytes(msg, 'utf-8'))
