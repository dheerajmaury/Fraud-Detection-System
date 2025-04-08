from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Create or get the Spark session
spark = SparkSession.builder \
    .appName("KafkaToParquetStreaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Kafka bootstrap servers and topic list (comma separated for multiple topics)
kafka_bootstrap_servers = "localhost:9092"
topics = "customers_topic,accounts_topic,transactions_topic"

# Read from Kafka as streaming source.
# All messages come in with the following columns: key, value, topic, partition, offset, timestamp, etc.
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topics) \
    .option("startingOffsets", "earliest") \
    .load()

# Cast binary 'value' and 'topic' columns to string
raw_df = raw_df.selectExpr("CAST(topic AS STRING) as topic", "CAST(value AS STRING) as value")

# Define JSON schemas for each topic. 
# Adjust these according to your actual JSON structure.
customers_schema = StructType() \
    .add("customer_id", StringType()) \
    .add("name", StringType()) \
    .add("email", StringType())

accounts_schema = StructType() \
    .add("account_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("balance", DoubleType())

transactions_schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("account_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("timestamp", StringType())

# Parse each topic's JSON according to its schema.
# Here we split the stream by topic so each branch applies the correct schema.
customers_df = raw_df.filter(col("topic") == "customers_topic") \
    .select(from_json(col("value"), customers_schema).alias("data")) \
    .select("data.*")

accounts_df = raw_df.filter(col("topic") == "accounts_topic") \
    .select(from_json(col("value"), accounts_schema).alias("data")) \
    .select("data.*")

transactions_df = raw_df.filter(col("topic") == "transactions_topic") \
    .select(from_json(col("value"), transactions_schema).alias("data")) \
    .select("data.*")

# Write streams to Parquet files.
# In streaming, you must specify a checkpoint directory to track progress.
# Output directories should be different for each branch.
customers_query = customers_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoints/customers") \
    .outputMode("append") \
    .start("/path/to/output/customers_parquet")

accounts_query = accounts_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoints/accounts") \
    .outputMode("append") \
    .start("/path/to/output/accounts_parquet")

transactions_query = transactions_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoints/transactions") \
    .outputMode("append") \
    .start("/path/to/output/transactions_parquet")

print("Spark streaming queries started. Waiting for data...")

# Await termination (in a real job you might implement a custom termination logic)
customers_query.awaitTermination()
accounts_query.awaitTermination()
transactions_query.awaitTermination()
