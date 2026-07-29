import psutil
import time
import json
import logging
import threading
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import subprocess

# ============================================================================
# EVENT HUBS CONFIGURATION (Kafka-compatible endpoint)
# ============================================================================
EVENT_HUBS_NAMESPACE = "<your-namespace>"  # e.g. "my-eh-namespace"
EVENT_HUBS_BROKER = f"{EVENT_HUBS_NAMESPACE}.servicebus.windows.net:9093"
EVENT_HUBS_CONNECTION_STRING = "<your-connection-string>"  # Endpoint=sb://...

KAFKA_METRICS_TOPIC = 'system-metrics'
KAFKA_LOGS_TOPIC = 'system-logs'

def send_payload(topic, payload):
    """Send payload to Event Hubs via Kafka protocol"""
    try:
        future = producer.send(topic, payload)
        # Block for 'synchronous' sends (optional)
        record_metadata = future.get(timeout=10)
        print(f"✅ Sent to {topic}: offset={record_metadata.offset}")
    except KafkaError as e:
        print(f"❌ Failed to send to Event Hubs: {e}")
        raise

# Initialize Kafka Producer with Event Hubs SASL configuration
print(f"Connecting to Event Hubs at {EVENT_HUBS_BROKER}...")

producer = KafkaProducer(
    bootstrap_servers=[EVENT_HUBS_BROKER],
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password=EVENT_HUBS_CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    # Additional settings for reliability
    retries=3,
    max_in_flight_requests_per_connection=1,
    acks='all'
)

print(f"✅ Connected to Event Hubs: {EVENT_HUBS_NAMESPACE}")

# Mac log sources (using 'log show' command)
# Note: This requires appropriate permissions. Some may require sudo.
LOG_SOURCES = {
    'system': 'subsystem == "com.apple.system"',
    'kernel': 'eventMessage contains "kernel"',
    'power': 'subsystem == "com.apple.power"',
    #'windowserver': 'process == "WindowServer"',
    'wifi': 'subsystem == "com.apple.wifi"',
    'bluetooth': 'subsystem == "com.apple.bluetooth"'
}

def get_metrics():
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    io_counters = psutil.disk_io_counters()

    return {
        'timestamp': time.time(),
        'type': 'metrics',
        'cpu_percent': cpu_percent,
        'memory': {
            'total': memory.total,
            'available': memory.available,
            'percent': memory.percent
        },
        'disk_io': {
            'read_bytes': io_counters.read_bytes,
            'write_bytes': io_counters.write_bytes
        }
    }

def stream_logs(category, predicate):
    """
    Tails macOS logs using 'log stream'
    """
    print(f"Starting log stream for: {category}")
    cmd = ['log', 'stream', '--predicate', predicate, '--style', 'json']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # We'll use a slightly different way to read logs that might be more robust
    while True:
        line = process.stdout.readline()
        if not line:
            break

        line = line.strip()

        # Log lines from 'log stream' can be complex.
        # It's outputting an array. We need to handle this properly.
        # Since log streaming is a continuous stream, simple line-by-line JSON parsing
        # won't work for arrays.
        # For terminal output, let's just print the raw line if it contains useful info.

        if any(keyword in line for keyword in ['"timestamp"', '"eventMessage"', '"category"']):
            print(f"Log Output: {line}")
            payload = {
                'timestamp': time.time(),
                'type': 'log',
                'category': category,
                'log_raw': line
            }
            send_payload(KAFKA_LOGS_TOPIC, payload)
    process.stdout.close()
    process.wait()

def main():
    print(f"Starting monitoring. Pushing to Event Hubs: {EVENT_HUBS_NAMESPACE}")
    print(f"  Metrics topic: {KAFKA_METRICS_TOPIC}")
    print(f"  Logs topic: {KAFKA_LOGS_TOPIC}")
    print("=" * 80)

    # Start log streamers in background threads
    for category, predicate in LOG_SOURCES.items():
        thread = threading.Thread(target=stream_logs, args=(category, predicate), daemon=True)
        thread.start()

    # Main thread sends metrics
    try:
        while True:
            metrics = get_metrics()
            send_payload(KAFKA_METRICS_TOPIC, metrics)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping...")
    finally:
        print("Closing producer...")
        producer.close()
        print("✅ Producer closed")

if __name__ == "__main__":
    main()
