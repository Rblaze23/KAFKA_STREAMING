from kafka import KafkaConsumer
import json
from time import sleep

# Configuration
KAFKA_BROKER = 'localhost:9093'
TOPIC_NAME = 'transactions-csv'
CONSUMER_GROUP = 'csv-processor-group'

# The value_deserializer converts the incoming bytes back to a string
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    group_id=CONSUMER_GROUP,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

print(f"Starting consumer for topic: {TOPIC_NAME} in group: {CONSUMER_GROUP}")

try:
    for message in consumer:
        csv_line = message.value

        # --- Phase 1: Clean Data (Basic Processing) ---

        # Split the line into fields
        fields = csv_line.split(',')

        # Simple validation: check if the expected number of fields (e.g., 4) are present
        EXPECTED_FIELD_COUNT = 4

        if len(fields) == EXPECTED_FIELD_COUNT:
            # Process the clean record
            transaction_id, customer_id, amount_str, timestamp = fields
            try:
                # Type conversion (clean data should succeed)
                amount = float(amount_str)
                print(
                    f"✅ CLEAN Record: ID={transaction_id}, Amount={amount:.2f}")

            except ValueError:
                # --- Phase 2: Dirty Data (Handling Type Errors) ---
                print(
                    f"DIRTY Type Error: Could not convert amount '{amount_str}' to float. Line: {csv_line}")
                # You could implement a 'dead letter queue' logic here

        else:
            # --- Phase 2: Dirty Data (Handling Structure Errors) ---
            print(
                f"DIRTY Structure Error: Expected {EXPECTED_FIELD_COUNT} fields, got {len(fields)}. Line: {csv_line}")
            # You could implement logging the malformed record here

except KeyboardInterrupt:
    pass
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    print("Consumer closed.")
