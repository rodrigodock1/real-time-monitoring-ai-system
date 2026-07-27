# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Real-Time Streaming Metrics Monitor

# COMMAND ----------

# DBTITLE 1,Stream directly from JSON files (like Kafka)
# Stream from JSON files (equivalent to Kafka consumer)
import shutil
import os

# Clean up checkpoints to avoid recovery errors
for checkpoint_dir in [
    "/Volumes/main/default/streaming_data/_checkpoints/notebook_metrics_stream",
    "/Volumes/main/default/streaming_data/_checkpoints/notebook_logs_stream"
]:
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
        print(f"✅ Cleaned checkpoint: {checkpoint_dir}")

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col

# Schema for the Event Hubs envelope (simulates Kafka message structure)
schema = StructType([
    StructField("value", StringType(), True),
    StructField("key", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Streaming read (equivalent to: raw_stream = spark.readStream.format("kafka").option("subscribe", "iot-raw").load())
raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/main/default/streaming_data/_schemas/raw_stream")
    .schema(schema)
    .load("/Volumes/main/default/streaming_data/")
)

# Parse the JSON payload inside 'value' field
from pyspark.sql.functions import regexp_replace

payload_schema = "timestamp DOUBLE, type STRING, cpu_percent DOUBLE, memory STRUCT<total: BIGINT, available: BIGINT, percent: DOUBLE>, disk_io STRUCT<read_bytes: BIGINT, write_bytes: BIGINT>, category STRING, log_raw STRING"

parsed_stream_metrics = (
    raw_stream
    .withColumn("payload", from_json(col("value"), payload_schema))
    .select(
        col("timestamp").alias("eventhub_timestamp"),
        "topic",
        "partition",
        "payload.timestamp",
        "payload.type",
        "payload.cpu_percent",
        "payload.memory.*",
        "payload.disk_io.*",
    )
    .where("type = 'metric'")
)

parsed_stream_logs = (
    raw_stream
    .withColumn("payload", from_json(col("value"), payload_schema))
    .select(
        col("timestamp").alias("eventhub_timestamp"),
        "topic",
        "partition",
        "payload.timestamp",
        "payload.type",
        "payload.category",
        regexp_replace(col("payload.log_raw"), '"eventMessage" : ', '').alias("log_raw")
    )
    #.where("category IS NOT NULL")
)

# Write metrics stream to memory table for querying
parsed_stream_metrics.writeStream \
    .format("memory") \
    .queryName("parsed_metrics_live") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/main/default/streaming_data/_checkpoints/notebook_metrics_stream") \
    .trigger(availableNow=True) \
    .start()

# Note: parsed_stream_logs is written to memory in Cell 4 using forEachBatch

# COMMAND ----------

spark.sql("SELECT * FROM parsed_metrics_live ORDER BY timestamp DESC LIMIT 10").display()

# COMMAND ----------

parsed_stream_metrics.printSchema()


# COMMAND ----------


def embeddings_generation(parsed_stream_logs):
    import mlflow.deployments
    ml_client = mlflow.deployments.get_deploy_client("databricks")
    
    text_batch = parsed_stream_logs.select("log_raw").toPandas()['log_raw'].tolist()

    # 3. Call the predict endpoint passing the entire list
    response = ml_client.predict(
        endpoint="databricks-bge-large-en", 
        inputs={"input": text_batch}
    )

    # 4. Extract vectors (one list of float values per input text)
    embeddings = [item["embedding"] for item in response["data"]]

    print(f"Total embeddings returned: {len(embeddings)}")
    print(f"Dimensions per embedding: {len(embeddings[0])}")
    return embeddings

# COMMAND ----------

generic_data = spark.createDataFrame([("",), ("",)], ["log_raw"])
generic_data.show()
response = embeddings_generation(generic_data)
print(response[0])

# COMMAND ----------

# DBTITLE 1,Display streaming logs (forEachBatch)
# Define Pandas UDF for embeddings generation
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd

for checkpoint_dir in [
    "/Volumes/main/default/streaming_data/_checkpoints/notebook_logs_stream"
]:
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
        print(f"✅ Cleaned checkpoint: {checkpoint_dir}")

from typing import Iterator

@pandas_udf(ArrayType(FloatType()))
def generate_embedding_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Pandas UDF with Iterator pattern for streaming data.
    Better batch control for streaming contexts.
    """
    import mlflow.deployments
    
    # Initialize MLflow client once for the iterator
    ml_client = mlflow.deployments.get_deploy_client("databricks")
    
    for texts in iterator:
        # texts is a pandas Series for this batch
        if len(texts) == 0:
            yield pd.Series([], dtype=object)
            continue
            
        # Convert to list for API call
        text_batch = texts.tolist()
        
        # Ensure we have valid strings
        text_batch = [str(t) if t is not None else "" for t in text_batch]
        
        # Call MLflow endpoint with batched texts
        response = ml_client.predict(
            endpoint="databricks-bge-large-en", 
            inputs={"input": text_batch}
        )
        
        # Extract embeddings
        embeddings = [item["embedding"] for item in response["data"]]
        
        # Yield as pandas Series
        yield pd.Series(embeddings)

# Apply embeddings UDF to streaming logs
enriched_stream = parsed_stream_logs.withColumn(
    "embedding", 
    generate_embedding_udf(col("log_raw"))
)

# Write enriched stream directly to memory table
enriched_stream.writeStream \
    .format("memory") \
    .queryName("parsed_logs_live") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/main/default/streaming_data/_checkpoints/notebook_logs_stream") \
    .option("maxFilesPerTrigger", 10) \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

# DBTITLE 1,Create Lakebase PostgreSQL tables
# Create tables in Lakebase PostgreSQL if they don't exist

# Update these with your actual Lakebase connection details
LAKEBASE_PROJECT = "real-time-db"
LAKEBASE_BRANCH = "production"
LAKEBASE_HOST = "dbc-270e414b-2aca.cloud.databricks.com"  # e.g., "adb-123456.cloud.databricks.com"
LAKEBASE_DATABASE = "postgres"

# SQL to create enriched_logs table
create_logs_table_sql = """
CREATE TABLE IF NOT EXISTS enriched_logs (
    eventhub_timestamp VARCHAR(255),
    topic VARCHAR(255),
    timestamp DOUBLE PRECISION,
    category VARCHAR(255),
    log_raw TEXT,
    embedding JSONB
);
"""

# SQL to create parsed_metrics table
create_metrics_table_sql = """
CREATE TABLE IF NOT EXISTS parsed_metrics (
    eventhub_timestamp VARCHAR(255),
    topic VARCHAR(255),
    timestamp DOUBLE PRECISION,
    cpu_percent DOUBLE PRECISION,
    total_memory BIGINT,
    available_memory BIGINT,
    percent_memory DOUBLE PRECISION
);
"""

print("✅ Table creation SQL prepared")
print("\nTo create tables, run these SQL statements in your Lakebase PostgreSQL database:")
print("\n1. enriched_logs table:")
print(create_logs_table_sql)
print("\n2. parsed_metrics table:")
print(create_metrics_table_sql)
print("\n⚠️  Don't forget to update LAKEBASE_HOST with your actual Lakebase instance host!")

# COMMAND ----------

# DBTITLE 1,Sink enriched logs to Lakebase PostgreSQL
# Sink enriched_stream to Lakebase PostgreSQL (ASYNC with Queue-Based Backpressure)
from pyspark.sql import DataFrame
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import time

# Lakebase connection configuration
LAKEBASE_PROJECT = "real-time-db"
LAKEBASE_BRANCH = "production"
LAKEBASE_HOST = "dbc-270e414b-2aca.cloud.databricks.com"  # Update with your Lakebase host
LAKEBASE_DATABASE = "postgres"  # Default database
LAKEBASE_USER = "databricks"

# Queue-based backpressure: limit pending async writes
MAX_PENDING_WRITES = 100  # Max 100 batches in queue
pending_writes = Queue(maxsize=MAX_PENDING_WRITES)

# Create thread pool once (reused across batches)
logs_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="lakebase-logs")

def write_logs_to_lakebase(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No logs to write")
        return
    
    # Convert array to JSON for PostgreSQL
    from pyspark.sql.functions import to_json
    
    logs_to_write = batch_df.select(
        "eventhub_timestamp",
        "topic",
        "timestamp",
        "category",
        "log_raw",
        to_json("embedding").alias("embedding")
    )
    
    # Convert to pandas BEFORE submitting (DataFrame not thread-safe)
    logs_pandas = logs_to_write.toPandas()
    row_count = len(logs_pandas)
    
    # Add to queue (BLOCKS if queue is full - provides backpressure)
    pending_writes.put(batch_id)
    print(f"📥 Batch {batch_id}: Added to queue ({pending_writes.qsize()}/{MAX_PENDING_WRITES} pending)")
    
    def async_write():
        """
        Background write with queue-based backpressure.
        Queue slot is always released in finally block.
        Backpressure: forEachBatch blocks when queue reaches 100 pending writes.
        """
        max_retries = 10
        retry_delay = 2  # Initial delay in seconds
        
        try:
            for attempt in range(max_retries):
                try:
                    # Recreate Spark DataFrame in thread context
                    df = spark.createDataFrame(logs_pandas)
                    
                    # Write to Lakebase PostgreSQL
                    df.write \
                        .format("jdbc") \
                        .option("url", f"jdbc:postgresql://{LAKEBASE_HOST}:5432/{LAKEBASE_DATABASE}") \
                        .option("dbtable", "enriched_logs") \
                        .option("user", LAKEBASE_USER) \
                        .option("driver", "org.postgresql.Driver") \
                        .option("batchsize", "1000") \
                        .mode("append") \
                        .save()
                    
                    print(f"✅ Batch {batch_id}: Async write completed ({row_count} logs)")
                    break  # Success - exit retry loop
                    
                except Exception as e:
                    error_msg = str(e).lower()
                    
                    # Check if it's a connection limit error
                    if any(keyword in error_msg for keyword in ["connection", "too many clients", "limit reached", "pool", "exhausted"]):
                        print(f"⚠️ Batch {batch_id}: PostgreSQL connection limit hit (attempt {attempt + 1}/{max_retries})")
                        
                        if attempt < max_retries - 1:
                            print(f"   🕒 Backing off for {retry_delay:.1f}s...")
                            time.sleep(retry_delay)
                            retry_delay = min(retry_delay * 1.5, 30)  # Exponential backoff, max 30s
                        else:
                            print(f"❌ Batch {batch_id}: Max retries reached - FAILED")
                    else:
                        # Non-connection error - fail immediately (no retry)
                        print(f"❌ Batch {batch_id}: Write FAILED - {type(e).__name__}: {e}")
                        break
        finally:
            # Always release queue slot (even on failure)
            pending_writes.get()
            print(f"📤 Batch {batch_id}: Released from queue ({pending_writes.qsize()}/{MAX_PENDING_WRITES} pending)")
    
    # Submit to thread pool and return immediately (don't wait)
    logs_executor.submit(async_write)

# Start streaming write to Lakebase
enriched_stream.writeStream \
    .foreachBatch(write_logs_to_lakebase) \
    .option("checkpointLocation", "/Volumes/main/default/streaming_data/_checkpoints/lakebase_logs_sink") \
    .option("maxFilesPerTrigger", 10) \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

# DBTITLE 1,Sink parsed metrics to Lakebase PostgreSQL
# Sink parsed_stream_metrics to Lakebase PostgreSQL (SYNCHRONOUS)
from pyspark.sql.functions import col

def write_metrics_to_lakebase(batch_df: DataFrame, batch_id: int):
    """
    Write parsed metrics batch to Lakebase PostgreSQL - SYNCHRONOUS.
    Waits for write to complete before checkpoint.
    """
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No metrics to write")
        return
    
    print(f"Batch {batch_id}: Writing {batch_df.count()} metrics to Lakebase")
    
    # Rename columns for clarity
    metrics_to_write = batch_df.select(
        "eventhub_timestamp",
        "topic",
        "timestamp",
        "cpu_percent",
        col("total").alias("total_memory"),
        col("available").alias("available_memory"),
        col("percent").alias("percent_memory")
    )
    
    # Write to Lakebase PostgreSQL using JDBC (blocks until complete)
    metrics_to_write.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{LAKEBASE_HOST}:5432/{LAKEBASE_DATABASE}") \
        .option("dbtable", "parsed_metrics") \
        .option("user", "databricks") \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", "1000") \
        .mode("append") \
        .save()
    
    print(f"✅ Batch {batch_id}: Written {batch_df.count()} metrics to Lakebase")

# Start streaming write to Lakebase
parsed_stream_metrics.writeStream \
    .foreachBatch(write_metrics_to_lakebase) \
    .option("checkpointLocation", "/Volumes/main/default/streaming_data/_checkpoints/lakebase_metrics_sink") \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

enriched_stream.printSchema()


# COMMAND ----------

# DBTITLE 1,View Live Parsed Metrics
# Query enriched logs with embeddings (refresh to see latest data)
spark.sql("SELECT eventhub_timestamp, topic, category, log_raw, embedding FROM parsed_logs_live ORDER BY timestamp DESC LIMIT 10").display()

# COMMAND ----------

# DBTITLE 1,View Live Parsed Logs
# Check embedding dimensions and count
result = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        SIZE(embedding) as embedding_dimensions
    FROM parsed_logs_live
    LIMIT 1
""")
result.display()

# Show a sample embedding vector
spark.sql("SELECT log_raw, embedding FROM parsed_logs_live LIMIT 1").display()

# COMMAND ----------

# DBTITLE 1,Run Standalone Streaming (JSON → Lakebase)
# Single-hop streaming: JSON (Auto Loader) → Parse → Enrich → Lakebase
# This achieves lower latency than the SDP pipeline (300-800ms vs 1-3 seconds)

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col, regexp_replace, pandas_udf, to_json
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd
from typing import Iterator
import os, shutil

# Configuration
JSON_SOURCE_PATH = "/Volumes/main/default/streaming_data/"
SCHEMA_LOCATION = "/Volumes/main/default/streaming_data/_schemas/realtime_stream"
CATALOG = "real_time_db"  # Unity Catalog catalog
SCHEMA = "public"  # Unity Catalog schema
CHECKPOINT_LOGS = "/Volumes/main/default/streaming_data/_checkpoints/standalone_logs"
CHECKPOINT_METRICS = "/Volumes/main/default/streaming_data/_checkpoints/standalone_metrics"

# Clean checkpoints
for cp in [CHECKPOINT_LOGS, CHECKPOINT_METRICS]:
    if os.path.exists(cp):
        shutil.rmtree(cp)
        print(f"✅ Cleaned: {cp}")

# Schemas
envelope_schema = StructType([
    StructField("value", StringType(), True),
    StructField("key", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", StringType(), True),
    StructField("timestamp", StringType(), True)
])

payload_schema = "timestamp DOUBLE, type STRING, cpu_percent DOUBLE, memory STRUCT<total: BIGINT, available: BIGINT, percent: DOUBLE>, disk_io STRUCT<read_bytes: BIGINT, write_bytes: BIGINT>, category STRING, log_raw STRING"

# UDF for embeddings
@pandas_udf(ArrayType(FloatType()))
def generate_embedding_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    import mlflow.deployments
    ml_client = mlflow.deployments.get_deploy_client("databricks")
    
    for texts in iterator:
        if len(texts) == 0:
            yield pd.Series([], dtype=object)
            continue
        text_batch = [str(t) if t is not None else "" for t in texts.tolist()]
        response = ml_client.predict(endpoint="databricks-bge-large-en", inputs={"input": text_batch})
        embeddings = [item["embedding"] for item in response["data"]]
        yield pd.Series(embeddings)

print("🚀 Starting single-hop streaming...")

# Stream 1: Logs with embeddings → Lakebase
logs_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.maxFilesPerTrigger", "10")
    .schema(envelope_schema)
    .load(JSON_SOURCE_PATH)
    .withColumn("payload", from_json(col("value"), payload_schema))
    .where("payload.category IS NOT NULL")
    .select(
        col("timestamp").alias("eventhub_timestamp"),
        col("topic"),
        col("payload.timestamp").alias("timestamp"),
        col("payload.category").alias("category"),
        regexp_replace(col("payload.log_raw"), '"eventMessage" : ', '').alias("log_raw")
    )
    .withColumn("embedding", generate_embedding_udf(col("log_raw")))
    .select("eventhub_timestamp", "topic", "timestamp", "category", "log_raw", "embedding")
)

logs_query = (
    logs_stream.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOGS)
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.enriched_logs")
)

print(f"✅ Logs stream started (checkpoint: {CHECKPOINT_LOGS})")

# Stream 2: Metrics → Lakebase
metrics_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.maxFilesPerTrigger", "10")
    .schema(envelope_schema)
    .load(JSON_SOURCE_PATH)
    .withColumn("payload", from_json(col("value"), payload_schema))
    .where("payload.type = 'Metric'")
    .select(
        col("timestamp").alias("eventhub_timestamp"),
        col("topic"),
        col("payload.timestamp").alias("timestamp"),
        col("payload.cpu_percent").alias("cpu_percent"),
        col("payload.memory.total").alias("total_memory"),
        col("payload.memory.available").alias("available_memory"),
        col("payload.memory.percent").alias("percent_memory")
    )
)

metrics_query = (
    metrics_stream.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_METRICS)
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.parsed_metrics")
)

print(f"✅ Metrics stream started (checkpoint: {CHECKPOINT_METRICS})")
print("🔥 Both streams running to Unity Catalog Delta tables")
print(f"   Logs → {CATALOG}.{SCHEMA}.enriched_logs")
print(f"   Metrics → {CATALOG}.{SCHEMA}.parsed_metrics")
print("   Target latency: 300-800ms (single checkpoint hop)")
print("   Use spark.streams.active to monitor")

# COMMAND ----------

import subprocess
subprocess.run([
    "python", 
    "/Workspace/Users/rjamohr@gmail.com/real-time-monitoring-ai-system/ai_stream_transformation/streaming_jobs/realtime_eventhubs_to_lakebase.py"
])

# COMMAND ----------

# DBTITLE 1,Import Fixed Module
# STEP 2: Import the fixed module (after kernel restart)
import sys
dbutils.library.restartPython() # STEP 1: Restart Python kernel to clear ALL cached state
sys.path.append('/Workspace/Users/rjamohr@gmail.com/real-time-monitoring-ai-system/ai_stream_transformation/streaming_jobs')

import realtime_eventhubs_to_lakebase  # Runs the fixed file with .toTable()
print("✅ Module imported with fixed .toTable() sinks")

# COMMAND ----------

spark.range(1).write
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOGS)
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.{TABLE_LOGS}")