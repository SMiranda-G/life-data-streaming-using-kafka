import csv
import config
import json
from confluent_kafka import Consumer

BOOTSTRAP_SERVER, SECURITY_PROTOCOL = config.config_values()

def sasl_conf():
    return {
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'security.protocol': SECURITY_PROTOCOL,
        'group.id': 'group2',  # ✅ Different group from MySQL consumers
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }

def main(topic):
    consumer = Consumer(sasl_conf())
    consumer.subscribe([topic])
    counter = 0
    print(f"File Writer started. Listening to: {topic}")

    with open('./output.csv', 'a+', newline='') as f:
        w = csv.writer(f)
        f.seek(0)
        if len(f.readlines()) == 0:
            f.seek(0)
            w.writerow(['name', 'price', 'bid_ts'])

        while True:
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                try:
                    bid = json.loads(msg.value().decode('utf-8'))
                    counter += 1
                    print(f"File Writer - Record {counter}: {bid}")
                    w.writerow([bid['name'], bid['price'], bid['bid_ts']])
                    f.flush()
                except json.JSONDecodeError as e:
                    print(f"JSON Parse Error: {e}")

            except KeyboardInterrupt:
                print("\nFile Writer stopped")
                break

    consumer.close()

if __name__ == "__main__":
    main("auction")