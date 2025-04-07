#!/usr/bin/env python3
# improved_produce_data.py - Enhanced synthetic data generator with risk flags, realistic fields, and Kafka integration

import os
import csv
import json
import random
import uuid
from datetime import timedelta
from faker import Faker
from kafka import KafkaProducer
import numpy as np

# Initialize Faker and seeds
fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)  

# Configuration
NUM_CUSTOMERS = 100
NUM_ACCOUNTS = 150
NUM_TRANSACTIONS = 1000

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Generate Customers
df_customers = []
for _ in range(NUM_CUSTOMERS):
    customer = {
        "customer_id": str(uuid.uuid4()),
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "address": fake.address().replace('\n', ', '),
        "dob": fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat(),
        "created_at": fake.date_time_this_decade().isoformat()
    }
    df_customers.append(customer)
    producer.send('customers_topic', customer)

# Generate Accounts
account_data = []
for _ in range(NUM_ACCOUNTS):
    cust = random.choice(df_customers)
    account = {
        "account_id": str(uuid.uuid4()),
        "customer_id": cust["customer_id"],
        "account_type": random.choice(["savings", "checking", "credit"]),
        "account_number": fake.unique.bban(),
        "created_at": fake.date_time_this_decade().isoformat(),
        "balance": round(random.uniform(1000, 100000), 2)
    }
    account_data.append(account)
    producer.send('accounts_topic', account)

# Generate Transactions
transaction_data = []
account_balances = {acc["account_id"]: acc["balance"] for acc in account_data}

for _ in range(NUM_TRANSACTIONS):
    account = random.choice(account_data)
    acct_id = account["account_id"]
    customer_id = account["customer_id"]

    txn_type = random.choice(["purchase", "transfer", "withdrawal", "deposit", "payment"])
    amount = round(random.uniform(5, 5000), 2)
    is_foreign = random.choices([True, False], weights=[0.1, 0.9])[0]
    is_high_risk_country = random.choices([True, False], weights=[0.05, 0.95])[0] if is_foreign else False

    opening = account_balances[acct_id]
    if txn_type in ["deposit", "payment"]:
        closing = opening + amount
    else:
        closing = opening - amount if opening - amount >= 0 else opening

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": acct_id,
        "timestamp": fake.date_time_this_year().isoformat(),
        "amount": amount,
        "transaction_type": txn_type,
        "merchant": fake.company(),
        "location": fake.city(),
        "is_foreign": is_foreign,
        "is_high_risk_country": is_high_risk_country,
        "opening_balance": round(opening, 2),
        "closing_balance": round(closing, 2)
    }

    transaction_data.append(transaction)
    account_balances[acct_id] = closing
    producer.send('transactions_topic', transaction)

# Flush all Kafka messages
producer.flush()

# Save to CSVs
def save_to_csv(filename, fieldnames, rows):
    file_exists = os.path.isfile(filename)
    with open(filename, mode='a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)

save_to_csv('customers.csv', df_customers[0].keys(), df_customers)
save_to_csv('accounts.csv', account_data[0].keys(), account_data)
save_to_csv('transactions.csv', transaction_data[0].keys(), transaction_data)

print(f"Generated {len(df_customers)} customers, {len(account_data)} accounts, and {len(transaction_data)} transactions.")
print("Data has been saved and sent to Kafka.")
