"""
HDFS Writer — Consumes cleaned Reddit data from Kafka processed topics
and writes it to HDFS as Parquet files.

Architecture:
    Kafka (reddit.posts.processed / reddit.comments.processed)
        ──▶ HDFS (/data/reddit/posts/... , /data/reddit/comments/...)

Data is buffered in memory and flushed to HDFS as Parquet files in batches.
Files are partitioned by: year / month / day / subreddit.

Directory structure:
    /data/reddit/posts/year=2026/month=02/day=18/subreddit=python/batch_001.parquet
    /data/reddit/comments/year=2026/month=02/day=18/subreddit=python/batch_001.parquet
"""
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from hdfs import InsecureClient
from confluent_kafka import Consumer, KafkaError

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hdfs-writer")

# ── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "hdfs-writer-group")
INPUT_TOPICS = os.getenv("INPUT_TOPICS", "reddit.posts.processed,reddit.comments.processed,reddit.post-updates.processed").split(",")

HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/data/reddit")

# Buffer settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))          # records per batch
FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "60"))  # max seconds between flushes

# ── Shutdown ────────────────────────────────────────────────────────────────
shutdown_event = threading.Event()


def _handle_signal(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Parquet schema definitions ──────────────────────────────────────────────
POST_SCHEMA = pa.schema([
    ("id", pa.string()),
    ("title", pa.string()),
    ("selftext", pa.string()),
    ("author", pa.string()),
    ("score", pa.int64()),
    ("upvote_ratio", pa.float64()),
    ("num_comments", pa.int64()),
    ("subreddit", pa.string()),
    ("subreddit_normalized", pa.string()),
    ("subreddit_id", pa.string()),
    ("created_utc", pa.string()),
    ("url", pa.string()),
    ("permalink", pa.string()),
    ("domain", pa.string()),
    ("is_self", pa.bool_()),
    ("is_video", pa.bool_()),
    ("over_18", pa.bool_()),
    ("spoiler", pa.bool_()),
    ("stickied", pa.bool_()),
    ("link_flair_text", pa.string()),
    ("author_flair_text", pa.string()),
    ("total_awards_received", pa.int64()),
    ("gilded", pa.int64()),
    ("distinguished", pa.string()),
    ("edited", pa.bool_()),
    ("locked", pa.bool_()),
    ("archived", pa.bool_()),
    # ── Image/media fields ──
    ("image_urls", pa.string()),             # JSON-encoded list of image URLs
    ("image_count", pa.int64()),
    ("media_type", pa.string()),             # text, image, gallery, video, link
    ("has_media", pa.bool_()),
    ("image_hdfs_paths", pa.string()),       # JSON-encoded list of HDFS paths where images are stored
    # ────────────────────────
    ("content_type", pa.string()),
    ("ingested_at", pa.string()),
])

COMMENT_SCHEMA = pa.schema([
    ("id", pa.string()),
    ("body", pa.string()),
    ("author", pa.string()),
    ("score", pa.int64()),
    ("subreddit", pa.string()),
    ("subreddit_normalized", pa.string()),
    ("subreddit_id", pa.string()),
    ("post_id", pa.string()),
    ("post_title", pa.string()),
    ("link_id", pa.string()),
    ("parent_id", pa.string()),
    ("is_top_level", pa.bool_()),
    ("depth", pa.int64()),
    ("created_utc", pa.string()),
    ("permalink", pa.string()),
    ("is_submitter", pa.bool_()),
    ("stickied", pa.bool_()),
    ("total_awards_received", pa.int64()),
    ("gilded", pa.int64()),
    ("distinguished", pa.string()),
    ("edited", pa.bool_()),
    ("author_flair_text", pa.string()),
    ("controversiality", pa.int64()),
    ("is_deleted", pa.bool_()),
    ("content_type", pa.string()),
    ("ingested_at", pa.string()),
])

POST_UPDATE_SCHEMA = pa.schema([
    ("id", pa.string()),
    ("original_score", pa.int64()),
    ("original_num_comments", pa.int64()),
    ("original_upvote_ratio", pa.float64()),
    ("updated_score", pa.int64()),
    ("updated_upvote_ratio", pa.float64()),
    ("updated_num_comments", pa.int64()),
    ("score_growth", pa.int64()),
    ("subreddit", pa.string()),
    ("created_utc", pa.string()),
    ("updated_at", pa.string()),
    ("content_type", pa.string()),
])


# ── HDFS helpers ────────────────────────────────────────────────────────────
def create_hdfs_client() -> InsecureClient:
    """Create and return an HDFS client."""
    return InsecureClient(HDFS_URL, user=HDFS_USER)


def wait_for_hdfs(timeout: int = 180):
    """Block until HDFS namenode is reachable."""
    logger.info("Waiting for HDFS at %s ...", HDFS_URL)
    start = time.time()
    while time.time() - start < timeout:
        try:
            client = create_hdfs_client()
            client.status("/")
            logger.info("HDFS is ready")
            return client
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError(f"HDFS not reachable at {HDFS_URL} after {timeout}s")


def ensure_hdfs_directory(client: InsecureClient, path: str):
    """Create an HDFS directory if it doesn't exist."""
    try:
        client.makedirs(path)
    except Exception:
        pass  # directory may already exist


# ── Image download helpers ──────────────────────────────────────────────────
IMAGE_EXT_MAP = {
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'image/webp': '.webp',
    'image/bmp': '.bmp',
}


def detect_image_extension(url: str, content_type: str = '') -> str:
    """Determine file extension from URL or Content-Type header."""
    # Try from Content-Type header first
    for mime, ext in IMAGE_EXT_MAP.items():
        if mime in content_type:
            return ext
    # Try from URL
    url_lower = url.lower().split('?')[0]  # remove query params
    for ext in ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'):
        if url_lower.endswith(ext):
            return ext
    return '.jpg'  # default fallback


def download_and_store_image(
    client: InsecureClient,
    image_url: str,
    post_id: str,
    index: int = 0,
) -> str | None:
    """
    Download a single image and store it in HDFS.
    Returns the HDFS path where the image was stored, or None on failure.
    """
    try:
        headers = {
            'User-Agent': 'reddit-pipeline-image-downloader/1.0',
            'Accept': 'image/*',
        }
        response = requests.get(image_url, timeout=15, headers=headers, stream=True)

        if response.status_code != 200:
            logger.warning(
                "Image download HTTP %d for post %s: %s",
                response.status_code, post_id, image_url[:100],
            )
            return None

        content_type = response.headers.get('Content-Type', '')
        if 'image' not in content_type and 'octet-stream' not in content_type:
            logger.warning(
                "Non-image Content-Type '%s' for post %s: %s",
                content_type, post_id, image_url[:100],
            )
            return None

        ext = detect_image_extension(image_url, content_type)
        now = datetime.now(timezone.utc)

        hdfs_dir = (
            f"{HDFS_BASE_PATH}/images/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        )
        ensure_hdfs_directory(client, hdfs_dir)

        filename = f"{post_id}_{index}{ext}"
        hdfs_path = f"{hdfs_dir}/{filename}"

        # Write image bytes to HDFS
        image_data = BytesIO(response.content)
        client.write(hdfs_path, data=image_data, overwrite=True)

        logger.info(
            "  Image stored: %s (%d KB)",
            hdfs_path, len(response.content) // 1024,
        )
        return hdfs_path

    except requests.exceptions.Timeout:
        logger.warning("Image download timeout for post %s: %s", post_id, image_url[:100])
        return None
    except Exception as e:
        logger.warning("Image download failed for post %s: %s — %s", post_id, image_url[:100], e)
        return None


def download_images_for_record(
    client: InsecureClient,
    record: dict,
) -> list[str]:
    """
    Download all images for a post record.
    Returns a list of HDFS paths where images were stored.
    """
    image_urls = record.get("image_urls", [])
    if not image_urls:
        return []

    # Handle case where image_urls is a JSON string (from Kafka)
    if isinstance(image_urls, str):
        try:
            image_urls = json.loads(image_urls)
        except (json.JSONDecodeError, TypeError):
            return []

    post_id = record.get("id", "unknown")
    hdfs_paths = []

    for i, url in enumerate(image_urls):
        path = download_and_store_image(client, url, post_id, index=i)
        if path:
            hdfs_paths.append(path)

    return hdfs_paths


def write_parquet_to_hdfs(
    client: InsecureClient,
    records: list[dict],
    content_type: str,
):
    """
    Write a batch of records to HDFS as a Parquet file.
    Partitioned by year/month/day.
    """
    if not records:
        return

    now = datetime.now(timezone.utc)
    batch_id = uuid.uuid4().hex[:12]

    # Choose schema
    if content_type == "post":
        schema = POST_SCHEMA
        base_dir = f"{HDFS_BASE_PATH}/posts"
    elif content_type == "post_update":
        schema = POST_UPDATE_SCHEMA
        base_dir = f"{HDFS_BASE_PATH}/post_updates"
    else:
        schema = COMMENT_SCHEMA
        base_dir = f"{HDFS_BASE_PATH}/comments"

    # Partition path
    partition_path = (
        f"{base_dir}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}"
    )

    ensure_hdfs_directory(client, partition_path)

    filename = f"batch_{batch_id}_{now.strftime('%H%M%S')}.parquet"
    full_path = f"{partition_path}/{filename}"

    # Build DataFrame and enforce schema
    df = pd.DataFrame(records)

    # ── For posts: download images and record HDFS paths ──
    if content_type == "post":
        all_hdfs_paths = []
        images_downloaded = 0
        for idx, record in enumerate(records):
            hdfs_paths = download_images_for_record(client, record)
            all_hdfs_paths.append(json.dumps(hdfs_paths) if hdfs_paths else "[]")
            images_downloaded += len(hdfs_paths)
        df["image_hdfs_paths"] = all_hdfs_paths

        # Convert image_urls list to JSON string for Parquet storage
        df["image_urls"] = df["image_urls"].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else (x if isinstance(x, str) else "[]")
        )

        if images_downloaded > 0:
            logger.info("  Downloaded %d images for %d posts", images_downloaded, len(records))

    # Only keep columns that are in the schema
    schema_names = [field.name for field in schema]
    for col in schema_names:
        if col not in df.columns:
            df[col] = None

    df = df[schema_names]

    # Convert to Arrow table
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    # Write to buffer
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    # Upload to HDFS
    client.write(full_path, data=buf, overwrite=True)

    logger.info(
        "Wrote %d %s records → %s (%d bytes)",
        len(records), content_type, full_path, buf.getbuffer().nbytes,
    )


# ── Kafka helpers ───────────────────────────────────────────────────────────
def wait_for_kafka(bootstrap_servers: str, timeout: int = 120):
    """Block until Kafka is reachable."""
    from confluent_kafka import Producer as KProducer

    logger.info("Waiting for Kafka at %s ...", bootstrap_servers)
    start = time.time()
    while time.time() - start < timeout:
        try:
            p = KProducer({"bootstrap.servers": bootstrap_servers})
            metadata = p.list_topics(timeout=5)
            if metadata.topics is not None:
                logger.info("Kafka is ready")
                return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError(f"Kafka not reachable after {timeout}s")


# ── Main loop ───────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  HDFS Writer — Starting")
    logger.info("=" * 60)

    wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)
    hdfs_client = wait_for_hdfs()

    # Ensure base directories
    for sub in ("posts", "comments", "images", "post_updates"):
        ensure_hdfs_directory(hdfs_client, f"{HDFS_BASE_PATH}/{sub}")

    # Kafka consumer
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(INPUT_TOPICS)
    logger.info("Subscribed to topics: %s", INPUT_TOPICS)

    # Buffers
    post_buffer: list[dict] = []
    comment_buffer: list[dict] = []
    post_update_buffer: list[dict] = []
    last_flush = time.time()
    total_written = 0

    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(timeout=1.0)

            if msg is not None and not msg.error():
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    content_type = data.get("content_type", "")

                    if content_type == "post":
                        post_buffer.append(data)
                    elif content_type == "comment":
                        comment_buffer.append(data)
                    elif content_type == "post_update":
                        post_update_buffer.append(data)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning("Failed to decode message: %s", e)
            elif msg is not None and msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Consumer error: %s", msg.error())

            # Flush conditions: batch size reached or time elapsed
            now = time.time()
            should_flush = (
                len(post_buffer) >= BATCH_SIZE
                or len(comment_buffer) >= BATCH_SIZE
                or len(post_update_buffer) >= BATCH_SIZE
                or (now - last_flush) >= FLUSH_INTERVAL_SECONDS
            )

            if should_flush and (post_buffer or comment_buffer or post_update_buffer):
                try:
                    if post_buffer:
                        write_parquet_to_hdfs(hdfs_client, post_buffer, "post")
                        total_written += len(post_buffer)
                        post_buffer.clear()

                    if comment_buffer:
                        write_parquet_to_hdfs(hdfs_client, comment_buffer, "comment")
                        total_written += len(comment_buffer)
                        comment_buffer.clear()

                    if post_update_buffer:
                        write_parquet_to_hdfs(hdfs_client, post_update_buffer, "post_update")
                        total_written += len(post_update_buffer)
                        post_update_buffer.clear()

                    last_flush = time.time()
                    logger.info("Total records written to HDFS: %d", total_written)
                except Exception as e:
                    logger.error("Failed to write to HDFS: %s", e)

    except KeyboardInterrupt:
        pass
    finally:
        # Final flush
        try:
            if post_buffer:
                write_parquet_to_hdfs(hdfs_client, post_buffer, "post")
            if comment_buffer:
                write_parquet_to_hdfs(hdfs_client, comment_buffer, "comment")
            if post_update_buffer:
                write_parquet_to_hdfs(hdfs_client, post_update_buffer, "post_update")
        except Exception as e:
            logger.error("Final flush failed: %s", e)

        consumer.close()
        logger.info("HDFS Writer shut down cleanly. Total: %d records", total_written)


if __name__ == "__main__":
    main()
