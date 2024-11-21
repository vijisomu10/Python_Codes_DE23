from kafka import KafkaProducer
import random
import time
import json
import pandas as pd
from datetime import datetime

excel_file = 'C:/Users/vijis/my_app_topic/product details.xlsx'
df = pd.read_excel(excel_file)
records = df.to_dict(orient='records')

order_id = random.randint(10000000, 99999999)

def order_create(order_id):        
    customer_id = random.randint(100000, 999999)
    order_placed_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    orderDetails = {
                    "order_id": order_id,
                    "product_details": random.choice(records),
                    "customer_id": customer_id,
                    "order_time": order_placed_time
                    }    
    return orderDetails

if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers = 'localhost:9092',
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
        )
    try:
        while True:
            time.sleep(10)
            slumpar_ungefär_25 = int(random.gauss(mu=25, sigma=2))
            for _ in range(slumpar_ungefär_25):            
                order_id += 1
                orderDetails = order_create(order_id)
                producer.send('ordersFake', orderDetails)
                print(orderDetails)
            
            producer.flush()            
    
    except KeyboardInterrupt:
        print('Shutting down')
    
    finally:
        producer.flush
        producer.close
