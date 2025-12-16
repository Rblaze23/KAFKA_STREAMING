from kafka import KafkaConsumer
import json
from time import sleep

# Configuration
KAFKA_BROKER = 'localhost:9093'
TOPIC_NAME = 'transactions-json' # Topic where the JSON producer wrote data
CONSUMER_GROUP = 'json-processor-group'

# The value_deserializer converts the incoming bytes to a string, then to a Python dict
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    group_id=CONSUMER_GROUP,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Starting JSON consumer for topic: {TOPIC_NAME} in group: {CONSUMER_GROUP}")
print("--- Listening for structured JSON events. Press Ctrl+C to stop. ---")

try:
    for message in consumer:
        event = message.value
        
        # 1. Data Quality Check (Handling the None from the producer in Phase 2)
        amount = event.get('amount')
        
        # --- CRITICAL FIX: Use 'user_id' instead of 'customer_id' ---
        user_id = event.get('user_id') 
        # ------------------------------------------------------------
        
        if amount is None:
            print(f"🔴 BAD EVENT: Amount is invalid/missing. ID={event.get('transaction_id')}. User: {user_id}")
            continue 
            
        # 2. Business Logic (e.g., filtering, aggregation)
        if amount > 400.0:
            print(f"💰 HIGH VALUE ALERT: Txn ID: {event['transaction_id']}, User: {user_id}, Amount: {amount:.2f}")
        else:
            print(f"✨ Processed Event: Txn ID: {event['transaction_id']}, User: {user_id}, Amount: {amount:.2f}")

except KeyboardInterrupt:
    pass
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    consumer.close()
    print("JSON Consumer closed.")