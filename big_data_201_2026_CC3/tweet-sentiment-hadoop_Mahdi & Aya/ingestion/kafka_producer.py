"""
ingestion/kafka_producer.py
-----------------------------
Simule un flux temps réel de tweets en lisant le dataset CSV
et en envoyant les tweets un par un dans un topic Kafka.
Aucune API Twitter requise.
"""

import time
import json
import argparse
import pandas as pd
from kafka import KafkaProducer
import yaml


def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def create_producer(broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def stream_tweets(csv_file: str, broker: str, topic: str, delay: float = 0.5):
    """Lit le CSV et envoie chaque tweet dans Kafka avec un délai."""
    df = pd.read_csv(csv_file)
    producer = create_producer(broker)

    print(f"[Kafka] Streaming {len(df)} tweets to topic '{topic}' ...")
    for _, row in df.iterrows():
        message = {
            "id": int(row["id"]),
            "text": str(row["text"]),
            "sentiment_label": str(row["sentiment"]),  # ground truth
        }
        producer.send(topic, value=message)
        print(f"  → Sent tweet {message['id']}: {message['text'][:60]}...")
        time.sleep(delay)

    producer.flush()
    producer.close()
    print("[Kafka] Done streaming.")


def main():
    parser = argparse.ArgumentParser(description="Kafka tweet producer (no API)")
    parser.add_argument("--file", default="data/tweets.csv")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Seconds between messages")
    args = parser.parse_args()

    cfg = load_config()
    stream_tweets(
        csv_file=args.file,
        broker=cfg["kafka"]["broker"],
        topic=cfg["kafka"]["topic"],
        delay=args.delay,
    )


if __name__ == "__main__":
    main()
