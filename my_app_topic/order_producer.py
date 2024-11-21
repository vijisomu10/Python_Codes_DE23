from kafka import KafkaProducer, KafkaConsumer
import random
import json
from datetime import datetime 

producer = KafkaProducer(bootstrap_servers='localhost:9092')

produkter = [
            ['Äpple', 'Frukt', 10, 'per kg'], 
            ['Mjölk', 'Mejeri', 1, 'per liter'],
            ['Banan', 'Frukt', 5, 'per kg'], 
            ['Bröd', 'Bröd', 2, 'per styck'],
            ['Kycklingbröst', 'Kött', 3, 'per kg'],
            ['Ris', 'Spannmål', 1, 'per kg'], 
            ['Pasta', 'Spannmål', 2, 'per paket'], 
            ['Tomater', 'Grönsak', 4, 'per kg'], 
            ['Choklad', 'Godis', 5, 'per styck'], 
            ['Vattenflaska', 'Dryck', 6, 'per styck']
            ]


for _ in range(10):
    order = {
            'orderid':random.randint(1111,9999),
            'orderdetaljer':random.choice(produkter),
            'kundid':random.randint(111111,999999),
            'ordertid': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
             }

    orderinfo_bytes = json.dumps(order).encode('utf-8')
    producer.send('orders', orderinfo_bytes)

producer.flush()
print('10 orders sent to Kafka')
