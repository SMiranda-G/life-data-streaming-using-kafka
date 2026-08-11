import datetime
import time
import mysql.connector as conn
import config
import json
from confluent_kafka import Consumer

BOOTSTRAP_SERVER, SECURITY_PROTOCOL = config.config_values()
def sasl_conf():
    return {
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'security.protocol': SECURITY_PROTOCOL,
        'group.id': 'group1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }

def main(topic):
    consumer = Consumer(sasl_conf())
    consumer.subscribe([topic])
    counter = 0
    print(f"Consumer 1 started. Listening to: {topic}")

    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            try:
                bid = json.loads(msg.value().decode('utf-8'))
                counter += 1
                print(f"\n{'='*50}")
                print(f"Consumer 1 - Record {counter}")
                print(f"Data: {bid}")
                print(f"Partition: {msg.partition()}, Offset: {msg.offset()}")

                name = bid['name']
                price = bid['price']
                bid_ts = bid['bid_ts']

                cnx = conn.connect(host="localhost", user="root", passwd="1234", database="test")
                cur = cnx.cursor()
                query = "INSERT INTO bid (name, price, bid_ts) VALUES (%s, %s, %s)"
                cur.execute(query, (name, price, bid_ts))
                cnx.commit()
                print(f"Consumer 1 - Inserted successfully!")
                cur.close()
                cnx.close()
                print(f"{'='*50}")

            except json.JSONDecodeError as e:
                print(f"JSON Parse Error: {e}")
            except conn.Error as err:
                print(f"MySQL Error: {err}")

        except KeyboardInterrupt:
            print("\nConsumer 1 stopped")
            break

    consumer.close()

if __name__ == "__main__":
    main("auction")