"""
Flink Processor — Consumes raw Reddit data from Kafka, cleans and transforms
it, and produces cleaned records to processed Kafka topics.

Architecture:
    Kafka (reddit.posts / reddit.comments)
        ──▶ Flink (cleaning & transformation)
        ──▶ Kafka (reddit.posts.processed / reddit.comments.processed)

Transformations applied:
    - Text cleaning (URLs, special characters, excessive whitespace)
    - HTML entity decoding
    - Timestamp normalization (UTC ISO-8601)
    - Empty/deleted content marking
    - Field validation and type enforcement
"""
import json
import logging
import os
import re
import signal
import sys
import threading
import time
import html as html_mod

from confluent_kafka import Consumer, Producer, KafkaError

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("flink-processor")

# ── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "flink-processor-group")
INPUT_TOPICS = os.getenv("INPUT_TOPICS", "reddit.posts,reddit.comments,reddit.post-updates").split(",")
OUTPUT_TOPIC_POSTS = os.getenv("OUTPUT_TOPIC_POSTS", "reddit.posts.processed")
OUTPUT_TOPIC_COMMENTS = os.getenv("OUTPUT_TOPIC_COMMENTS", "reddit.comments.processed")
OUTPUT_TOPIC_POST_UPDATES = os.getenv("OUTPUT_TOPIC_POST_UPDATES", "reddit.post-updates.processed")
BATCH_TIMEOUT_MS = int(os.getenv("BATCH_TIMEOUT_MS", "1000"))

# ── Shutdown ────────────────────────────────────────────────────────────────
shutdown_event = threading.Event()


def _handle_signal(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ── Compiled regex patterns ─────────────────────────────────────────────────
URL_PATTERN = re.compile(r"https?://\S+|www\.\S+")
SPECIAL_CHARS_PATTERN = re.compile(r"[^\w\s.,!?;:'\"-/()@#$%&*+=\[\]{}|\\<>~`^]", re.UNICODE)
MULTI_SPACE_PATTERN = re.compile(r"\s+")
REDDIT_LINK_PATTERN = re.compile(r"\[([^\]]+)\]\([^)]+\)")  # [text](url) → text
REDDIT_MARKDOWN_PATTERN = re.compile(r"[*_~^]+")


# ── Text cleaning functions ─────────────────────────────────────────────────
def clean_text(text: str) -> str:
    """Apply all text cleaning transformations."""
    if not text or text in ("[deleted]", "[removed]"):
        return text

    # Decode HTML entities (e.g. &amp; → &)
    text = html_mod.unescape(text)

    # Convert Reddit markdown links to plain text
    text = REDDIT_LINK_PATTERN.sub(r"\1", text)

    # Remove remaining markdown formatting
    text = REDDIT_MARKDOWN_PATTERN.sub("", text)

    # Remove URLs
    text = URL_PATTERN.sub("", text)

    # Normalize whitespace
    text = MULTI_SPACE_PATTERN.sub(" ", text).strip()

    return text


def clean_post(data: dict) -> dict:
    """Clean and transform a Reddit post record."""
    cleaned = data.copy()

    # Clean text fields
    cleaned["title"] = clean_text(data.get("title", ""))
    cleaned["selftext"] = clean_text(data.get("selftext", ""))

    # Normalize author
    author = data.get("author", "")
    if not author or author in ("[deleted]", "None", ""):
        cleaned["author"] = "[deleted]"

    # Ensure numeric fields
    for field in ("score", "num_comments", "total_awards_received", "gilded"):
        cleaned[field] = int(data.get(field, 0) or 0)

    cleaned["upvote_ratio"] = float(data.get("upvote_ratio", 0.0) or 0.0)

    # Ensure boolean fields
    for field in ("is_self", "is_video", "over_18", "spoiler", "stickied",
                  "edited", "locked", "archived"):
        cleaned[field] = bool(data.get(field, False))

    # Mark empty self-text
    if not cleaned["selftext"]:
        cleaned["selftext"] = ""

    # Subreddit normalization (lowercase)
    cleaned["subreddit_normalized"] = data.get("subreddit", "").lower()

    # ── Preserve image/media fields (DO NOT clean these) ──
    cleaned["image_urls"] = data.get("image_urls", [])
    cleaned["image_count"] = int(data.get("image_count", 0) or 0)
    cleaned["media_type"] = data.get("media_type", "text")
    cleaned["has_media"] = bool(data.get("has_media", False))

    # Mark content as cleaned
    cleaned["_cleaned"] = True
    cleaned["content_type"] = "post"

    return cleaned


def clean_comment(data: dict) -> dict:
    """Clean and transform a Reddit comment record."""
    cleaned = data.copy()

    # Clean text fields
    cleaned["body"] = clean_text(data.get("body", ""))

    # Clean post_title if present (linked from producer)
    if data.get("post_title"):
        cleaned["post_title"] = clean_text(data["post_title"])

    # Preserve post linkage fields
    cleaned["post_id"] = data.get("post_id", "")
    cleaned["is_top_level"] = bool(data.get("is_top_level", False))
    cleaned["depth"] = int(data.get("depth", 0) or 0)

    # Normalize author
    author = data.get("author", "")
    if not author or author in ("[deleted]", "None", ""):
        cleaned["author"] = "[deleted]"

    # Mark deleted/removed bodies
    body = cleaned["body"]
    if body in ("[deleted]", "[removed]", ""):
        cleaned["is_deleted"] = True
    else:
        cleaned["is_deleted"] = False

    # Ensure numeric fields
    for field in ("score", "total_awards_received", "gilded", "controversiality"):
        cleaned[field] = int(data.get(field, 0) or 0)

    # Ensure boolean fields
    for field in ("is_submitter", "stickied", "edited"):
        cleaned[field] = bool(data.get(field, False))

    # Subreddit normalization
    cleaned["subreddit_normalized"] = data.get("subreddit", "").lower()

    # Mark content as cleaned
    cleaned["_cleaned"] = True
    cleaned["content_type"] = "comment"

    return cleaned


def clean_post_update(data: dict) -> dict:
    """Clean and validate a post-update record (updated metrics from fetch-comments)."""
    cleaned = data.copy()

    # Ensure numeric fields
    for field in ("original_score", "updated_score", "score_growth",
                  "original_num_comments", "updated_num_comments"):
        cleaned[field] = int(data.get(field, 0) or 0)

    for field in ("original_upvote_ratio", "updated_upvote_ratio"):
        cleaned[field] = float(data.get(field, 0.0) or 0.0)

    cleaned["_cleaned"] = True
    cleaned["content_type"] = "post_update"

    return cleaned


def process_message(raw_value: bytes) -> tuple[dict | None, str | None]:
    """
    Parse a raw Kafka message, clean it, and return (cleaned_data, output_topic).
    Returns (None, None) if the message cannot be processed.
    """
    try:
        data = json.loads(raw_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning("Failed to decode message: %s", e)
        return None, None

    content_type = data.get("content_type", "")

    if content_type == "post":
        cleaned = clean_post(data)
        return cleaned, OUTPUT_TOPIC_POSTS
    elif content_type == "comment":
        cleaned = clean_comment(data)
        return cleaned, OUTPUT_TOPIC_COMMENTS
    elif content_type == "post_update":
        cleaned = clean_post_update(data)
        return cleaned, OUTPUT_TOPIC_POST_UPDATES
    else:
        logger.warning("Unknown content_type: %s", content_type)
        return None, None


# ── Kafka helpers ───────────────────────────────────────────────────────────
def wait_for_kafka(bootstrap_servers: str, timeout: int = 120):
    """Block until Kafka is reachable."""
    logger.info("Waiting for Kafka at %s ...", bootstrap_servers)
    start = time.time()
    while time.time() - start < timeout:
        try:
            p = Producer({"bootstrap.servers": bootstrap_servers})
            metadata = p.list_topics(timeout=5)
            if metadata.topics is not None:
                logger.info("Kafka is ready")
                return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError(f"Kafka not reachable after {timeout}s")


def delivery_callback(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)


# ── Main processing loop ───────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  Flink Processor — Starting")
    logger.info("=" * 60)

    wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }
    consumer = Consumer(consumer_conf)

    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "flink-processor",
        "acks": "all",
        "compression.type": "snappy",
        "linger.ms": 50,
    }
    producer = Producer(producer_conf)

    consumer.subscribe(INPUT_TOPICS)
    logger.info("Subscribed to topics: %s", INPUT_TOPICS)

    processed_count = 0
    error_count = 0

    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(timeout=BATCH_TIMEOUT_MS / 1000.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue

            cleaned, output_topic = process_message(msg.value())

            if cleaned is not None and output_topic is not None:
                try:
                    value = json.dumps(cleaned, ensure_ascii=False).encode("utf-8")
                    key = cleaned.get("id", "").encode("utf-8")
                    producer.produce(
                        output_topic,
                        key=key,
                        value=value,
                        callback=delivery_callback,
                    )
                    producer.poll(0)
                    processed_count += 1

                    if processed_count % 100 == 0:
                        logger.info(
                            "Processed %d messages (%d errors)",
                            processed_count, error_count,
                        )
                except Exception as e:
                    error_count += 1
                    logger.error("Failed to produce cleaned message: %s", e)
            else:
                error_count += 1

    except KeyboardInterrupt:
        pass
    finally:
        logger.info(
            "Shutting down. Total processed: %d, errors: %d",
            processed_count, error_count,
        )
        consumer.close()
        producer.flush(timeout=10)
        logger.info("Flink Processor shut down cleanly.")


if __name__ == "__main__":
    main()
