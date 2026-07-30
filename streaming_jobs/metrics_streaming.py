# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Configuration & Setup
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col, regexp_replace, pandas_udf, from_utc_timestamp
from pyspark.sql.types import ArrayType, DoubleType
import pandas as pd
from typing import Iterator
import os

# ============================================================================
# DATA SOURCE CONFIGURATION
# ============================================================================
USE_JSON_FILES = False  # Set to False when switching to Event Hubs

# Declare widgets with defaults (for both job and interactive use)
dbutils.widgets.text("event_hubs_namespace", "real-time-metrics")
dbutils.widgets.text("event_hubs_topic", "system_data")
dbutils.widgets.text("lakebase_db", "workspace")
dbutils.widgets.text("lakebase_schema", "default")
dbutils.widgets.text("volume_catalog", "dev")  # 'dev' for development, 'main' for production

# Get volume catalog for environment-aware paths
VOLUME_CATALOG = dbutils.widgets.get("volume_catalog")

# JSON files configuration (for testing)
JSON_SOURCE_PATH = f"/Volumes/{VOLUME_CATALOG}/default/streaming_data/"
SCHEMA_LOCATION = f"/Volumes/{VOLUME_CATALOG}/default/streaming_data/_schemas/realtime_stream"

# Event Hubs configuration (for production)
EVENT_HUBS_NAMESPACE = dbutils.widgets.get("event_hubs_namespace")
EVENT_HUBS_TOPIC = dbutils.widgets.get("event_hubs_topic")
# Retrieve connection string and verify
EVENT_HUBS_CONNECTION_STRING = dbutils.secrets.get(scope="real-time", key="event_hubs_connection_string")

# Unity Catalog configuration (Lakebase)
CATALOG = dbutils.widgets.get("lakebase_db")  # Unity Catalog catalog
SCHEMA = dbutils.widgets.get("lakebase_schema")  # Unity Catalog schema
# Log text pattern
pattern = r'"eventMessage"\s*:\s*|["\,]'

# Checkpoint locations (separate for each mode to avoid format conflicts)
if USE_JSON_FILES:
    CHECKPOINT_LOGS = f"/Volumes/{VOLUME_CATALOG}/default/streaming_data/_checkpoints/json_logs"
    CHECKPOINT_METRICS = f"/Volumes/{VOLUME_CATALOG}/default/streaming_data/_checkpoints/json_metrics"
else:
    CHECKPOINT_LOGS = f"/Volumes/{VOLUME_CATALOG}/default/streaming_data/_checkpoints/eventhubs_logs"
    CHECKPOINT_METRICS = f"/Volumes/{VOLUME_CATALOG}/default/streaming_data/_checkpoints/eventhubs_metrics"

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
@pandas_udf(ArrayType(DoubleType()))
def generate_embedding_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    import mlflow.deployments
    import pandas as pd
    import time
    ml_client = mlflow.deployments.get_deploy_client("databricks")
    batch_size = 100  # must be <= 150

    for texts in iterator:
        text_batch = [
            str(t) if t is not None else ""
            for t in texts.tolist()
        ]
        
        all_embeddings = []

        for i in range(0, len(text_batch), batch_size):
            chunk = text_batch[i:i + batch_size]

            print(f"Sending embedding batch of size {len(chunk)}")

            start = time.time()

            response = ml_client.predict(
                endpoint="databricks-bge-large-en",
                inputs={"input": chunk}
            )

            embeddings = [
                [float(x) for x in item["embedding"]]
                for item in response["data"]
            ]

            print(f"Embedding finished in {time.time()-start:.2f}s")

            all_embeddings.extend(embeddings)

        yield pd.Series(all_embeddings)

print("🚀 Configuration loaded")
print(f"   Environment: {VOLUME_CATALOG} (volumes at /Volumes/{VOLUME_CATALOG}/default/...)")
print(f"   Mode: {'JSON Files (Auto Loader)' if USE_JSON_FILES else 'Event Hubs (Kafka)'}")
print(f"   Logs checkpoint: {CHECKPOINT_LOGS}")
print(f"   Metrics checkpoint: {CHECKPOINT_METRICS}")

# COMMAND ----------

# DBTITLE 1,Stream Logs with Embeddings
# ============================================================================
# Stream 1: Logs → Lakebase (with embeddings)
# ============================================================================
print("🚀 Starting logs stream...")
print(f"   Source: {'JSON files' if USE_JSON_FILES else 'Event Hubs'}")

if USE_JSON_FILES:
    # JSON files via Auto Loader (for testing)
    logs_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.maxFilesPerTrigger", "10")
        .schema(envelope_schema)
        .load(JSON_SOURCE_PATH)
        .withColumn("payload", from_json(col("value"), payload_schema))
        .where("payload.type = 'log'")
        .select(
            col("timestamp").alias("eventhub_timestamp"),
            col("topic"),
            col("payload.timestamp").alias("timestamp"),
            col("payload.category").alias("category"),
            regexp_replace(col("payload.log_raw"), '"eventMessage" : ', '').alias("log_raw")
        )
        .withColumn("embedding_vector", generate_embedding_udf(col("log_raw")))
        .select("eventhub_timestamp", "topic", "timestamp", "category", "log_raw", "embedding_vector")
    )
else:
    # Event Hubs via Kafka (for production)
    logs_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", f"{EVENT_HUBS_NAMESPACE}.servicebus.windows.net:9093")
        .option("subscribe", EVENT_HUBS_TOPIC)
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", 
                f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENT_HUBS_CONNECTION_STRING}";')
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "100")
        .option("failOnDataLoss", "false")
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "30000")
        .load()
        .withColumn("payload", from_json(col("value").cast("string"), payload_schema))
        .where("payload.type = 'log'")
        .select(
            from_utc_timestamp(col("timestamp"), "Europe/Berlin").alias("eventhub_timestamp"),
            col("topic"),
            col("payload.timestamp").alias("timestamp"),
            col("payload.category").alias("category"),
            regexp_replace(col("payload.log_raw"), pattern, "").alias("log_raw")
        )
        .withColumn("embedding_vector", generate_embedding_udf(col("log_raw")))
        .select("eventhub_timestamp", "topic", "timestamp", "category", "log_raw", "embedding_vector")
    )

logs_query = (
    logs_stream.writeStream
    .format("postgresql")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_LOGS}/checkpoint2")
    .trigger(processingTime="100 milliseconds")
    .toTable(f"{CATALOG}.{SCHEMA}.enriched_logs")
)

logs_query.awaitTermination(timeout=10)
print(f"✅ Logs stream started (checkpoint: {CHECKPOINT_LOGS})")

# COMMAND ----------

# DBTITLE 1,Continuous Metrics Streaming
print("🔥 Starting continuous metrics streaming (while loop)...")
print(f"   Source: {'JSON files' if USE_JSON_FILES else 'Event Hubs'}")
print(f"   Path: {JSON_SOURCE_PATH if USE_JSON_FILES else EVENT_HUBS_NAMESPACE}")
print(f"   Target: {CATALOG}.{SCHEMA}.parsed_metrics")
print("=" * 80)

try:
    # Stream 2: Metrics → Lakebase
    if USE_JSON_FILES:
        # JSON files via Auto Loader (for testing)
        metrics_stream = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
            .option("cloudFiles.maxFilesPerTrigger", "10")
            .schema(envelope_schema)
            .load(JSON_SOURCE_PATH)
            .withColumn("payload", from_json(col("value"), payload_schema))
            .where("payload.type = 'metrics'")
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
    else:
        # Event Hubs via Kafka (for production)
        metrics_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{EVENT_HUBS_NAMESPACE}.servicebus.windows.net:9093")
            .option("subscribe", EVENT_HUBS_TOPIC)
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.jaas.config", 
                    f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENT_HUBS_CONNECTION_STRING}";')
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", "10000")
            .load()
            .withColumn("payload", from_json(col("value").cast("string"), payload_schema))
            .where("payload.type = 'metrics'")
            .select(
                from_utc_timestamp(col("timestamp"), "Europe/Berlin").alias("eventhub_timestamp"),
                col("topic"),
                col("payload.timestamp").alias("timestamp"),
                col("payload.cpu_percent").alias("cpu_percent"),
                col("payload.memory.total").alias("total_memory"),
                col("payload.memory.available").alias("available_memory"),
                col("payload.memory.percent").alias("percent_memory")
            )
        )
    
    # Process one batch
    metrics_query = (
        metrics_stream.writeStream
        .format("postgresql")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_METRICS)
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.parsed_metrics")
    )
    # Wait for this batch to complete
    metrics_query.awaitTermination(timeout=10)
    
except KeyboardInterrupt:
    print(f"\n\n✅ Stopped")
except Exception as e:
    print(f"\n\n❌ Error: {e}")
    raise

# COMMAND ----------