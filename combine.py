from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CombineParquetData") \
    .getOrCreate()

# Load Parquet files
df_customers = spark.read.parquet("parquet/customers") \
    .withColumnRenamed("created_at", "created_at_cust")

df_accounts = spark.read.parquet("parquet/accounts") \
    .withColumnRenamed("account_type", "account_type_acct") \
    .withColumnRenamed("created_at", "created_at_acct")

df_transactions = spark.read.parquet("parquet/transactions") \
    .withColumnRenamed("account_type", "account_type_txn")

# Perform joins
df_joined = df_transactions \
    .join(df_accounts, on="account_id", how="inner") \
    .join(df_customers, on="customer_id", how="inner")

# Select final ordered columns
final_columns = [
    "transaction_id", "timestamp", "amount", "transaction_type",
    "merchant", "location", "is_foreign", "is_high_risk_country",
    "opening_balance", "closing_balance",

    "account_id", "account_type_txn", "account_type_acct", "account_number", "balance", "created_at_acct",

    "customer_id", "name", "email", "phone", "address", "dob", "created_at_cust",

    "is_fraud", "fraud_reasons"
]

df_ordered = df_joined.select(*final_columns)

# Save to denormalized CSV
output_path = "denormalized_transactions.csv"
if not os.path.exists(output_path):
    df_ordered.coalesce(1).write.csv(output_path, header=True, mode='overwrite')
else:
    df_ordered.coalesce(1).write.csv(output_path, header=False, mode='append')

print(f"✅ Denormalized dataset written to '{output_path}'.")

# Stop Spark session
spark.stop()
