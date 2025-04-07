import json
import os
import pandas as pd
from kafka import KafkaConsumer

# Expected number of messages (should match the producer's configuration)
EXPECTED_CUSTOMERS = 100
EXPECTED_ACCOUNTS = 150
EXPECTED_TRANSACTIONS = 1000

# Initialize Kafka consumer to listen to three topics with JSON deserialization
consumer = KafkaConsumer(
    'customers_topic', 'accounts_topic', 'transactions_topic',
    bootstrap_servers='localhost:9092',
    group_id='parquet-consumer-group',
    auto_offset_reset='earliest',  # start at beginning of topic if no offset
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # decode bytes to JSON dict
)

print("Consumer started. Waiting for messages... (will exit when expected messages are received)")

# Lists to collect messages for each topic
customers_data = []
accounts_data = []
transactions_data = []

try:
    for message in consumer:
        # message.value is already a Python dict due to the deserializer
        data = message.value
        topic = message.topic

        # Store the record in the corresponding list
        if topic == 'customers_topic':
            customers_data.append(data)
        elif topic == 'accounts_topic':
            accounts_data.append(data)
        elif topic == 'transactions_topic':
            transactions_data.append(data)

        # (Optional) print confirmation for each received message
        print(f"Received message from {topic}: {data}")

        # Check if all expected messages have been received
        if (len(customers_data) >= EXPECTED_CUSTOMERS and 
            len(accounts_data) >= EXPECTED_ACCOUNTS and 
            len(transactions_data) >= EXPECTED_TRANSACTIONS):
            print("Expected number of messages received. Exiting consumer loop.")
            break
except KeyboardInterrupt:
    # Allow the user to break out of the loop with Ctrl+C
    pass
finally:
    # On exit, write each list to a Parquet file regardless of whether data was received
    pd.DataFrame(customers_data).to_parquet('customers.parquet', engine='pyarrow', index=False)
    pd.DataFrame(accounts_data).to_parquet('accounts.parquet', engine='pyarrow', index=False)
    pd.DataFrame(transactions_data).to_parquet('transactions.parquet', engine='pyarrow', index=False)
    print("Consumer stopped. Parquet files have been written to:", os.getcwd())
