#!/usr/bin/env python3
"""
Event Hubs JSON Generator - Real-Time Simulation

Generates metrics and log JSON messages continuously and writes them to files.
Auto Loader can then consume these files with Structured Streaming.

Usage:
    python generate_eventhubs_json.py [output_path]
    
Example:
    python generate_eventhubs_json.py /Volumes/main/default/streaming_data
    
Press Ctrl+C to stop.
"""

import json
import time
import random
import os
import sys
from datetime import datetime
from pathlib import Path


def generate_metrics():
    """Generate a metrics JSON message"""
    return {
        "timestamp": time.time(),
        "type": "metrics",
        "cpu_percent": round(random.uniform(0, 100), 1),
        "memory": {
            "total": 17179869184,
            "available": random.randint(2000000000, 12000000000),
            "percent": round(random.uniform(0, 100), 1)
        },
        "disk_io": {
            "read_bytes": random.randint(10000000000000, 30000000000000),
            "write_bytes": random.randint(2000000000000, 7000000000000)
        }
    }


def generate_log():
    """Generate a log JSON message"""
    # Pick a category
    category_rand = random.random()
    if category_rand < 0.4:
        category = "kernel"
        message = f'\"eventMessage\" : \"SK[{random.randint(0, 10)}]: flow_entry_alloc ...\"'
    elif category_rand < 0.8:
        category = "system"
        message = f'\"eventMessage\" : \"System process PID {random.randint(0, 10000)} started with code 0\"'
    else:
        category = "wifi"
        signal = random.randint(40, 90)
        ssid = random.randint(0, 100)
        message = f'\"eventMessage\" : \"WiFi connection to SSID {ssid} established with signal strength -{signal}dBm\"'
    
    return {
        "timestamp": time.time(),
        "type": "log",
        "category": category,
        "log_raw": message
    }


def generate_eventhub_message():
    """Generate a complete Event Hubs message with metadata"""
    # 50% metrics, 50% logs
    if random.random() < 0.5:
        payload = generate_metrics()
    else:
        payload = generate_log()
    
    return {
        "value": json.dumps(payload),  # JSON payload as string (would be binary in real Event Hubs)
        "key": None,
        "topic": "system-monitoring",
        "partition": random.randint(0, 3),
        "offset": None,
        "timestamp": datetime.now().isoformat()
    }


def main():
    """Main loop - generates and writes messages to JSON files continuously"""
    # ALWAYS use the streaming data volume path
    output_path = "/Volumes/main/default/streaming_data"
    print(f"\n📍 Output path: {output_path}")
    
    # Create directory if it doesn't exist
    try:
        Path(output_path).mkdir(parents=True, exist_ok=True)
        print(f"✅ Output directory ready: {output_path}")
    except Exception as e:
        print(f"❌ Failed to create directory: {e}")
        return
    
    print("\n🚀 Starting Event Hubs JSON Generator")
    print(f"   Output: {output_path}")
    print("   Rate: 10 messages/file, 1 file every 2 seconds")
    print("   Format: JSON Lines (one JSON object per line)")
    print("   Press Ctrl+C to stop\n")
    print("=" * 80)
    
    message_count = 0
    file_count = 0
    
    try:
        while True:
            # Collect 10 messages per file
            batch = []
            for _ in range(10):
                message = generate_eventhub_message()
                message_count += 1
                batch.append(message)
            
            # Write to file (JSON Lines format - one JSON object per line)
            file_count += 1
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:19]  # Include milliseconds
            filename = f"eventhubs_{timestamp}_{file_count:04d}.json"
            filepath = os.path.join(output_path, filename)
            
            with open(filepath, 'w') as f:
                for msg in batch:
                    f.write(json.dumps(msg) + '\n')
            
            print(f"[File {file_count:4d}] {filename} | {len(batch)} messages | Total: {message_count}")
            
            # Wait 2 seconds before next batch
            time.sleep(2)
            
    except KeyboardInterrupt:
        print(f"\n\n✅ Stopped. Generated {message_count} messages in {file_count} files.")
        print(f"   Output: {output_path}")


if __name__ == "__main__":
    main()
