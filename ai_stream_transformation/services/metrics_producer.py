import psutil
import time
import json
import logging
import threading
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import subprocess

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_METRICS_TOPIC = 'system-metrics'
KAFKA_LOGS_TOPIC = 'system-logs'

# Add a flag to fallback to terminal output
USE_TERMINAL_FALLBACK = False

def send_payload(topic, payload):
    global USE_TERMINAL_FALLBACK
    if USE_TERMINAL_FALLBACK:
        print(f"Fallback [Terminal] ({topic}): {json.dumps(payload)}")
        return

    try:
        producer.send(topic, payload)
    except Exception as e:
        print(f"Failed to send to Kafka: {e}. Switching to terminal fallback.")
        USE_TERMINAL_FALLBACK = True
        print(f"Fallback [Terminal] ({topic}): {json.dumps(payload)}")

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Could not connect to Kafka at {KAFKA_BROKER}: {e}. Falling back to terminal output.")
    USE_TERMINAL_FALLBACK = True
    producer = None

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
    print(f"Starting monitoring. Pushing to {KAFKA_BROKER}. Metrics to {KAFKA_METRICS_TOPIC}, Logs to {KAFKA_LOGS_TOPIC}")

    # Start log streamers in background threads
    for category, predicate in LOG_SOURCES.items():
        thread = threading.Thread(target=stream_logs, args=(category, predicate), daemon=True)
        thread.start()

    # Main thread sends metrics
    try:
        while True:
            metrics = get_metrics()
            send_payload(KAFKA_METRICS_TOPIC, metrics)
            time.sleep(1) # Changed to 1s as requested
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
