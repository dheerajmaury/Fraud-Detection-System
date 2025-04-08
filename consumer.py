from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, BooleanType, DoubleType

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaTransactionsConsumer") \
    .getOrCreate()

# Define schema for transactions
transaction_schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("account_id", StringType()) \
    .add("account_type", StringType()) \
    .add("timestamp", StringType()) \
    .add("amount", DoubleType()) \
    .add("transaction_type", StringType()) \
    .add("merchant", StringType()) \
    .add("location", StringType()) \
    .add("is_foreign", BooleanType()) \
    .add("is_high_risk_country", BooleanType()) \
    .add("opening_balance", DoubleType()) \
    .add("closing_balance", DoubleType()) \
    .add("is_fraud", BooleanType()) \
    .add("fraud_reasons", StringType())

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value column from binary to string and parse JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .withColumn("data", from_json(col("json_string"), transaction_schema)) \
    .select("data.*")

# Write to Parquet (you can change this to 'console' for debugging)
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "parquet/transactions") \
    .option("checkpointLocation", "parquet/transactions_checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
