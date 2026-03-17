import time
import json
import random
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load variables from .env
load_dotenv()

conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('API_KEY'),
    'sasl.password': os.getenv('API_SECRET')
}

producer = Producer(conf)

topic = "spindle_sensors"

def simulate_spindle(spindle_id):
    weight = 100.0
    while True:
        # 1. Normal Operation: Weight increases slightly
        weight += random.uniform(0.5, 2.0)
        
        # 2. Randomly trigger a "Break" (5% chance)
        if random.random() < 0.05:
            print(f"!!! BREAK DETECTED ON {spindle_id} !!!")
            weight = 0.0  # Sudden drop
            
        data = {
            "spindle_id": spindle_id,
            "weight_grams": round(weight, 2),
            "status": "RUNNING" if weight > 0 else "BROKEN",
            "timestamp_ms": int(time.time() * 1000)
        }
        
        producer.produce(topic, value=json.dumps(data))
        producer.flush()
        
        if weight == 0:
            time.sleep(5) # Wait 5 seconds to simulate downtime
            weight = 100.0 # Reset
        
        time.sleep(1) # Send data every second

simulate_spindle("SPINDLE-01")