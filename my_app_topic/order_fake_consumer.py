from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer('ordersFake',
                         bootstrap_servers = 'localhost:9092',
                         auto_offset_reset = 'earliest',
                         enable_auto_commit = True,
                         group_id = 'my-processed-consumer',
                         value_deserializer = lambda x: json.loads(x.decode('utf-8')),
                         auto_commit_interval_ms = 3000
                         )

try: 
    while True:
        time.sleep(3)

        messages = consumer.poll(10)
        if messages:             
            orderDetails = []
            for key,value in messages.items():
                for msg in value:
                    orderDetails.append(msg.value)
        
            with open('consumed_orders.txt', 'a') as file:    
                file.write(json.dumps(orderDetails, indent=4) + '\n')
            print(f"Total new orders written in the file are {len(orderDetails)}")

except Exception as e:
    print(f"Error: {e}")
except KeyboardInterrupt:
    print('Keyboard interruption')
finally:
    consumer.close()
  