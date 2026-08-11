from flask import Flask, render_template, request, redirect, url_for
import datetime
import random
import config
import mysql.connector as conn
from uuid import uuid4
from confluent_kafka import Producer
import json
import traceback

app = Flask(__name__)

# SIMPLIFIED: Only get what we need
BOOTSTRAP_SERVER, SECURITY_PROTOCOL = config.config_values()

print("=" * 50)
print("Flask Application Starting...")
print(f"Kafka Bootstrap Server: {BOOTSTRAP_SERVER}")
print(f"Security Protocol: {SECURITY_PROTOCOL}")
print("=" * 50)

def get_highest_bid():
    """
    Retrieve the maximum bid price from the MySQL database.
    
    Returns:
        int: The highest bid price, or 0 if no bids exist or an error occurs.
    """
    print("Fetching highest bid from MySQL...")
    try:
        cnx = conn.connect(
            host="localhost",
            user="root",
            passwd="1234",
            database="test"
        )
        print("MySQL connection established successfully.")
        cur = cnx.cursor()
        cur.execute("SELECT MAX(price) FROM bid;")
        result = cur.fetchone()[0]
        cur.close()
        cnx.close()
        print(f"Highest bid: {result if result else 0}")
        return result if result else 0
    except Exception as e:
        print(f"MySQL error: {e}")
        print(traceback.format_exc())
        return 0

def sasl_conf():
    """
    Configure Kafka connection settings using SASL authentication.
    
    Returns:
        dict: Kafka producer configuration with bootstrap servers and security protocol.
    """
    return {
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'security.protocol': SECURITY_PROTOCOL,
    }

def delivery_report(err, msg):
    """
    Callback function that reports the status of message delivery to Kafka.
    
    Args:
        err: Error object if delivery failed, None if successful.
        msg: The message object containing topic, partition, and offset information.
    """
    if err is not None:
        print(f"Delivery failed: {err}")
        return
    print(f"Successfully produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

@app.route("/", methods=['GET', 'POST'])
def bid():
    """
    Handle bid submissions and display the bidding interface.
    
    GET: Render the bid form with the current highest bid.
    POST: Process the submitted bid, publish to Kafka, and redirect back.
    
    Returns:
        Rendered HTML template with bid status and highest bid information.
    """
    print("\n" + "=" * 50)
    print(f"Request received: {request.method}")
    
    result = get_highest_bid()

    if request.method == 'POST':
        print("Processing POST request...")
        print(f"Form data: {request.form}")
        
        name = request.form.get('name', '').strip()
        price = request.form.get('price', '').strip()
        bid_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg_key = str(uuid4())

        try:
            price_int = int(price)
        except ValueError:
            print(f"Failed to convert price to int: {price}")
            price_int = 0

        response = {
            'name': name,
            'price': price_int,
            'bid_ts': bid_ts
        }

        try:
            producer = Producer(sasl_conf())
            producer.produce(
                topic='auction',
                key=msg_key,
                value=json.dumps(response).encode('utf-8'),
                on_delivery=delivery_report
            )
            producer.flush()
            print("Message produced successfully.")
            return redirect(url_for('bid', success='true'))

        except Exception as e:
            print(f"Kafka error: {e}")
            return redirect(url_for('bid', success='false'))

    bid_added = request.args.get('success') == 'true'
    return render_template('index.html', bid_added=bid_added, highest_bid=result)

if __name__ == '__main__':
    print("Starting Flask application on http://127.0.0.1:5000")
    app.run(debug=True)