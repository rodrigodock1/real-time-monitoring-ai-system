# Real-Time Streaming: Event Hubs → Lakebase (< 200ms latency target)
# Single flattened query for minimal latency

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col, regexp_replace, pandas_udf
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd
from typing import Iterator

# Initialize Spark session
spark = SparkSession.builder \
    .appName("realtime-eventhubs-lakebase") \
    .getOrCreate()

# Data source configuration (JSON files for testing, Event Hubs later)
USE_JSON_FILES = True  # Set to False when switching to Event Hubs
JSON_SOURCE_PATH = "/Volumes/main/default/streaming_data/"
SCHEMA_LOCATION = "/Volumes/main/default/streaming_data/_schemas/realtime_stream"

# Event Hubs configuration (for production)
EVENT_HUBS_NAMESPACE = "<your-namespace>"  # e.g. "my-eh-namespace"
EVENT_HUBS_TOPIC = "logs-topic"
EVENT_HUBS_CONNECTION_STRING = "<your-connection-string>"  # Endpoint=sb://...

# Unity Catalog configuration (Lakebase)
CATALOG = "real_time_db"  # UC catalog (maps to Lakebase project)
SCHEMA = "public"  # UC schema (maps to Lakebase schema)
TABLE_LOGS = "enriched_logs"
TABLE_METRICS = "parsed_metrics"

# Checkpoint locations
CHECKPOINT_LOGS = "/Volumes/main/default/streaming_data/_checkpoints/realtime_logs"
CHECKPOINT_METRICS = "/Volumes/main/default/streaming_data/_checkpoints/realtime_metrics"

# Payload schema
payload_schema = "timestamp DOUBLE, type STRING, cpu_percent DOUBLE, memory STRUCT<total: BIGINT, available: BIGINT, percent: DOUBLE>, disk_io STRUCT<read_bytes: BIGINT, write_bytes: BIGINT>, category STRING, log_raw STRING"

# ============================================================================
# Embeddings UDF (MLflow)
# ============================================================================

@pandas_udf(ArrayType(FloatType()))
def generate_embedding_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Generate embeddings using MLflow endpoint.
    Iterator pattern for streaming efficiency.
    """
    import mlflow.deployments
    
    ml_client = mlflow.deployments.get_deploy_client("databricks")
    
    for texts in iterator:
        if len(texts) == 0:
            yield pd.Series([], dtype=object)
            continue
            
        text_batch = [str(t) if t is not None else "" for t in texts.tolist()]
        
        response = ml_client.predict(
            endpoint="databricks-bge-large-en", 
            inputs={"input": text_batch}
        )
        
        embeddings = [item["embedding"] for item in response["data"]]
        yield pd.Series(embeddings)

# ============================================================================
# Stream 1: Logs → Lakebase (with embeddings)
# ============================================================================

print("🚀 Starting real-time logs stream... WITH ALL MODIFICATIONS!!!!!!!!!!!!")

if USE_JSON_FILES:
    # JSON files via Auto Loader (for testing)
    envelope_schema = StructType([
        StructField("value", StringType(), True),
        StructField("key", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
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
        .withColumn("embedding", generate_embedding_udf(col("log_raw")))
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
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENT_HUBS_CONNECTION_STRING}";')
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "100")
        .load()
        .withColumn("payload", from_json(col("value").cast("string"), payload_schema))
        .where("payload.type = 'log'")
        .select(
            col("timestamp").alias("eventhub_timestamp"),
            col("topic"),
            col("payload.timestamp").alias("timestamp"),
            col("payload.category").alias("category"),
            regexp_replace(col("payload.log_raw"), '"eventMessage" : ', '').alias("log_raw")
        )
        .withColumn("embedding", generate_embedding_udf(col("log_raw")))
    )

# Write to Unity Catalog Delta table
logs_query = (
    logs_stream.writeStream
    .format("postgresql")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOGS)
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.{TABLE_LOGS}")
)

print(f"✅ Logs stream started (checkpoint: {CHECKPOINT_LOGS})")

# ============================================================================
# Stream 2: Metrics → Lakebase (no enrichment)
# ============================================================================

print("🚀 Starting real-time metrics stream...")

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
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENT_HUBS_CONNECTION_STRING}";')
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "100")
        .load()
        .withColumn("payload", from_json(col("value").cast("string"), payload_schema))
        .where("payload.type = 'metric'")
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

# Write to Unity Catalog Delta table
metrics_query = (
    metrics_stream.writeStream
    .format("postgresql")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_METRICS)
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.{TABLE_METRICS}")
)

print(f"✅ Metrics stream started (checkpoint: {CHECKPOINT_METRICS})")

# ============================================================================
# Wait for termination
# ============================================================================

source = "JSON files (Auto Loader)" if USE_JSON_FILES else "Event Hubs (Kafka)"
print(f"🔥 Both streams running. Target latency: < 200ms")
print(f"   Source: {source}")
print(f"   Logs: {source} → Parse → Enrich (MLflow) → {CATALOG}.{SCHEMA}.{TABLE_LOGS}")
print(f"   Metrics: {source} → Parse → {CATALOG}.{SCHEMA}.{TABLE_METRICS}")
print("")
print("Press Ctrl+C to stop...")

spark.streams.awaitAnyTermination()