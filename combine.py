import os
import pandas as pd

# Load Parquet files
df_customers = pd.read_parquet("customers.parquet", engine='pyarrow')
df_accounts = pd.read_parquet("accounts.parquet", engine='pyarrow')
df_transactions = pd.read_parquet("transactions.parquet", engine='pyarrow')

# Merge transactions ← accounts ← customers
merged_df = df_transactions \
    .merge(df_accounts, on="account_id", suffixes=('_txn', '_acct')) \
    .merge(df_customers, on="customer_id", suffixes=('', '_cust'))

# Optional: Reorder columns (grouping fields logically)
ordered_columns = [
    "transaction_id", "timestamp", "amount", "transaction_type",
    "merchant", "location", "is_foreign", "is_high_risk_country",
    "opening_balance", "closing_balance",

    "account_id", "account_type", "account_number", "balance", "created_at_acct",

    "customer_id", "name", "email", "phone", "address", "dob", "created_at"
]

# Apply reordering if all columns present
merged_df = merged_df[[col for col in ordered_columns if col in merged_df.columns]]

# Append the denormalized data to the CSV file
output_file = "denormalized_transactions.csv"
# If file doesn't exist, write the header; otherwise, do not
header = not os.path.exists(output_file)
merged_df.to_csv(output_file, mode='a', index=False, header=header)

print(f"✅ Denormalized dataset appended to '{output_file}' with {len(merged_df)} records.")
