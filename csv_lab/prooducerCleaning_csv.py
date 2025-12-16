import csv
import time
from kafka import KafkaProducer

# Configuration
# Matches the advertised listener in docker-compose.yml
KAFKA_BROKER = 'localhost:9093'
TOPIC_NAME = 'transactions-csv'
CLEAN_DATA_FILE = 'csv_lab/data/transactions.csv'
DIRTY_DATA_FILE = 'transactions_dirty.csv'
# Choose which file to stream based on the phase
DATA_FILE = DIRTY_DATA_FILE

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    # Key and value are sent as bytes
    value_serializer=lambda v: str(v).encode('utf-8')
)

print(f"Starting producer for topic: {TOPIC_NAME} using file: {DATA_FILE}")

try:
    with open(DATA_FILE, mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        # Skip the header row
        header = next(csv_reader)
        print(f"Header: {header}")

        # Stream the data
        for i, row in enumerate(csv_reader):
            # Row is a list of strings, join them back into a single CSV line
            csv_line = ','.join(row)

            # Send the raw CSV line as the message value
            future = producer.send(TOPIC_NAME, value=csv_line)

            # Optional: Log the message and wait for send confirmation
            try:
                record_metadata = future.get(timeout=10)
                # print(f"Sent: {csv_line} to partition {record_metadata.partition}, offset {record_metadata.offset}")
            except Exception as e:
                print(f"Error sending message: {e}")

            time.sleep(0.1)  # Slow down for visibility

except FileNotFoundError:
    print(f"Error: Data file {DATA_FILE} not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    producer.flush()
    producer.close()
    print("Producer finished and closed.")

# Note: For Phase 2 (Dirty Data), you'll switch DATA_FILE to DIRTY_DATA_FILE.
# The producer code itself does not need to change much, as it is just streaming lines.
