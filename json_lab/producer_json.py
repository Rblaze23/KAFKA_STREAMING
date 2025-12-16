import csv
import json
import time
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'localhost:9093'
# New topic for structured data
TOPIC_NAME = 'transactions-json'
# Use the correct, fixed path
CLEAN_DATA_FILE = 'transactions.csv'
DIRTY_DATA_FILE = 'transactions_dirty.csv'
DATA_FILE = CLEAN_DATA_FILE

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    # Convert the Python dict (the event) into a JSON byte string
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def process_and_send(row, header):
    """
    Takes a CSV row, processes it, and sends as a JSON event.
    """

    try:
        # Assuming header: ['transaction_id', 'user_id', 'amount', 'timestamp']
        transaction_id = row[0]
        customer_id = row[1]
        amount_str = row[2]
        timestamp = row[3]

        # Convert types (This is the critical step for structure)
        # For clean data (Phase 1), this should always succeed.
        try:
            amount_value = float(amount_str)
        except ValueError:
            amount_value = None  # Placeholder for dirty data handling in Phase 2

        # Create the structured JSON event (Python dict)
        event = {
            "transaction_id": transaction_id,
            "user_id": customer_id,
            "amount": amount_value,
            "timestamp": timestamp,
            "processing_time": time.time()
        }

        # Send the JSON event, using user_id as the key for partitioning
        producer.send(TOPIC_NAME, value=event,
                      key=str(customer_id).encode('utf-8'))

    except IndexError:
        print(f"Skipping malformed row (too few fields): {row}")
    except Exception as e:
        print(f"Unexpected error processing row {row}: {e}")


print(
    f"Starting JSON producer for topic: {TOPIC_NAME} using file: {DATA_FILE}")

try:
    with open(DATA_FILE, mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)
        print(f"Reading with Header: {header}")

        for row in csv_reader:
            process_and_send(row, header)
            time.sleep(0.05)  # Speed up slightly

except FileNotFoundError:
    print(f"Error: Data file {DATA_FILE} not found. Check path.")
finally:
    producer.flush()
    producer.close()
    print("JSON Producer finished and closed.")
