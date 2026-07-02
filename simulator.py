import json
import time
import random
import numpy as np
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def create_producer():
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka.")
            return producer
        except NoBrokersAvailable:
            print(f"  Kafka not ready, retrying ({attempt+1}/10)...")
            time.sleep(3)
    raise Exception("Could not connect to Kafka after 10 attempts.")


def make_metric(tick, service="payment-service"):
    is_spike = (tick % 90) < 15

    if is_spike:
        cpu        = round(random.gauss(88, 3), 2)
        latency    = round(random.gauss(2400, 80), 2)
        error_rate = round(max(0, random.gauss(11, 0.8)), 2)
        restarts   = random.randint(2, 5)
    else:
        cpu        = round(random.gauss(34, 4), 2)
        latency    = round(random.gauss(115, 12), 2)
        error_rate = round(max(0, random.gauss(0.3, 0.1)), 2)
        restarts   = 0

    return {
        "timestamp":           time.time(),
        "service":             service,
        "cpu_percent":         cpu,
        "latency_p99_ms":      latency,
        "error_rate_percent":  error_rate,
        "pod_restarts":        restarts,
        "is_spike":            is_spike
    }


if __name__ == "__main__":
    producer = create_producer()
    tick = 0
    print("Simulator running. Spike injects every 90s for 15s. Ctrl+C to stop.\n")

    while True:
        metric = make_metric(tick)
        producer.send('metrics-raw', value=metric)

        label = "SPIKE" if metric["is_spike"] else "normal"
        print(
            f"[{label:6s}] "
            f"CPU={metric['cpu_percent']:5.1f}%  "
            f"Latency={metric['latency_p99_ms']:7.1f}ms  "
            f"Errors={metric['error_rate_percent']:4.1f}%  "
            f"Restarts={metric['pod_restarts']}"
        )

        time.sleep(10)
        tick += 10