# Real-Time Monitoring AI System

Streaming data ingestion and processing system for real-time metrics and logs.

## Architecture

* **Source**: Azure Event Hubs (Kafka-compatible) or JSON files (for testing)
* **Processing**: Databricks Spark Structured Streaming
* **Destination**: Lakebase PostgreSQL (Unity Catalog)
* **Enrichment**: Text embeddings via Databricks BGE-Large-EN endpoint
* **Storage**: Environment-isolated volumes
  * Dev: `/Volumes/dev/default/streaming_data/`
  * Prod: `/Volumes/main/default/streaming_data/`

## Components

### Pipeline: `real-time-streaming`

Spark Declarative Pipeline (serverless) for continuous ingestion:

* Ingests logs and metrics from Event Hubs
* Generates embeddings for log text
* Writes to Unity Catalog tables

### Job: `real_time_ai_processing`

Continuous job that runs the metrics_streaming notebook:

* Processes metrics data from Event Hubs
* Streams to Lakebase PostgreSQL
* Runs continuously with retry on failure

## Deployment

This project uses Databricks Declarative Automation Bundles (DAB).

### Prerequisites

1. Databricks CLI installed and authenticated
2. Secrets configured in scope `real-time`:
   * `event_hubs_connection_string`: Azure Event Hubs connection string
   * `redis_broker_url`: Redis broker URL (for Celery)
   * `redis_backend_url`: Redis backend URL (for Celery)

### Deploy to Development

```bash
# Validate configuration
databricks bundle validate

# Deploy to dev workspace
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run real_time_streaming -t dev

# Start the job
databricks bundle run real_time_ai_processing -t dev
```

### Deploy to Production

```bash
# Deploy to production
databricks bundle deploy -t prod

# Run the pipeline
databricks bundle run real_time_streaming -t prod

# Start the job
databricks bundle run real_time_ai_processing -t prod
```

## Configuration

Edit `databricks.yml` to customize:

* **Catalog/Schema**: Update target-specific `variables.catalog` and `variables.schema`
* **Volume Catalog**: Each target sets `volume_catalog` for environment isolation
  * Dev: `volume_catalog: dev` → uses `/Volumes/dev/default/...`
  * Prod: `volume_catalog: main` → uses `/Volumes/main/default/...`
* **Event Hubs**: Update `variables.event_hubs_namespace` and `variables.event_hubs_topic`
* **Pipeline settings**: Modify `resources.pipelines.real_time_streaming`
* **Job settings**: Modify `resources.jobs.real_time_ai_processing`

## Monitoring

* **Pipeline runs**: Check the Databricks UI under Lakeflow → Pipelines
* **Job runs**: Check the Databricks UI under Workflows → Jobs
* **Checkpoints**: Stored in `/Volumes/{env}/default/streaming_data/_checkpoints/`
  * Dev: `/Volumes/dev/default/streaming_data/_checkpoints/`
  * Prod: `/Volumes/main/default/streaming_data/_checkpoints/`

## Troubleshooting

### Checkpoint Recovery Errors

Checkpoint directories are environment-isolated:
* Dev: `/Volumes/dev/default/streaming_data/_checkpoints/`
* Prod: `/Volumes/main/default/streaming_data/_checkpoints/`

If you encounter incompatible checkpoint errors after changing stream configurations, you may need to reset the checkpoint state for that environment.

### Switch Between JSON and Event Hubs

Edit Cell 1 in `streaming_jobs/metrics_streaming`:

```python
USE_JSON_FILES = False  # Set to True for JSON testing, False for Event Hubs
```

## Development

### Local Testing

1. Generate test data:
   ```bash
   python services/generate_eventhubs_json.py
   ```

2. Set `USE_JSON_FILES = True` in the notebook
3. Run cells interactively in Databricks notebook UI

### Project Structure

```
.
├── databricks.yml                # Bundle configuration
├── .databricks/
│   └── project.json             # Project metadata
├── streaming_jobs/
│   └── metrics_streaming        # Main streaming notebook
├── services/
│   ├── metrics_producer.py      # Event producer
│   └── generate_eventhubs_json.py  # Test data generator
└── README.md                    # This file
```