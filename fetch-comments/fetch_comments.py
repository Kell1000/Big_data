"""
Fetch Comments — Standalone script to fetch comments for collected posts.

Run this manually whenever you want:
    docker compose run --rm fetch-comments

How it works:
    1. Reads posts from Kafka topic 'reddit.posts' (using its own consumer group)
    2. For each post NOT already processed (tracked via checkpoint file):
       - Fetches ALL comments from Reddit API
       - Publishes each comment to Kafka 'reddit.comments' (linked to post)
    3. Saves progress to checkpoint file after each post
    4. On next run: automatically skips posts already processed

Checkpoint file: /app/data/checkpoint.json
    - Stored in a Docker volume so it persists across runs
    - Contains: set of processed post IDs + stats
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import praw
from confluent_kafka import Consumer, Producer, KafkaError, TopicPartition

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fetch-comments")

# ── Config ──────────────────────────────────────────────────────────────────
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "reddit-pipeline-comment-fetcher")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_POSTS = os.getenv("KAFKA_TOPIC_POSTS", "reddit.posts")
KAFKA_TOPIC_COMMENTS = os.getenv("KAFKA_TOPIC_COMMENTS", "reddit.comments")
KAFKA_TOPIC_POST_UPDATES = os.getenv("KAFKA_TOPIC_POST_UPDATES", "reddit.post-updates")

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/app/data")
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, "checkpoint.json")

# Rate limiting: seconds to wait between API calls per post
API_DELAY = float(os.getenv("API_DELAY", "2.0"))

# ── Graceful shutdown ───────────────────────────────────────────────────────
should_stop = False


def _handle_signal(signum, frame):
    global should_stop
    logger.info("Shutdown signal received — will stop after current post")
    should_stop = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Checkpoint management ───────────────────────────────────────────────────
def load_checkpoint() -> dict:
    """
    Load checkpoint from disk.
    Returns dict with:
        - processed_ids: set of post IDs already processed
        - total_comments: total comments fetched across all runs
        - last_run: timestamp of the last run
    """
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                data = json.load(f)
            checkpoint = {
                "processed_ids": set(data.get("processed_ids", [])),
                "total_comments": data.get("total_comments", 0),
                "last_run": data.get("last_run", None),
            }
            logger.info(
                "Checkpoint loaded: %d posts already processed, %d total comments",
                len(checkpoint["processed_ids"]),
                checkpoint["total_comments"],
            )
            return checkpoint
        except Exception as e:
            logger.warning("Failed to load checkpoint: %s — starting fresh", e)

    return {"processed_ids": set(), "total_comments": 0, "last_run": None}


def save_checkpoint(checkpoint: dict):
    """Save checkpoint to disk."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    data = {
        "processed_ids": sorted(list(checkpoint["processed_ids"])),
        "total_comments": checkpoint["total_comments"],
        "last_run": datetime.now(timezone.utc).isoformat(),
        "processed_count": len(checkpoint["processed_ids"]),
    }
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    logger.debug("Checkpoint saved: %d posts processed", len(checkpoint["processed_ids"]))


# ── Reddit helpers ──────────────────────────────────────────────────────────
def create_reddit() -> praw.Reddit:
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )
    reddit.read_only = True
    return reddit


def extract_comment_data(comment, post_id: str, post_title: str, post_subreddit: str) -> dict:
    """Extract comment fields with post linkage."""
    return {
        "id": comment.id,
        "body": comment.body or "",
        "author": str(comment.author) if comment.author else "[deleted]",
        "score": comment.score,
        "subreddit": post_subreddit,
        "subreddit_id": getattr(comment, "subreddit_id", ""),
        "post_id": post_id,
        "post_title": post_title,
        "link_id": comment.link_id,
        "parent_id": comment.parent_id,
        "is_top_level": comment.parent_id == f"t3_{post_id}",
        "depth": getattr(comment, "depth", 0),
        "created_utc": datetime.fromtimestamp(comment.created_utc, tz=timezone.utc).isoformat(),
        "permalink": f"https://reddit.com{comment.permalink}",
        "is_submitter": comment.is_submitter,
        "stickied": comment.stickied,
        "total_awards_received": comment.total_awards_received,
        "gilded": comment.gilded,
        "distinguished": comment.distinguished,
        "edited": bool(comment.edited),
        "author_flair_text": comment.author_flair_text,
        "controversiality": comment.controversiality,
        "content_type": "comment",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Kafka helpers ───────────────────────────────────────────────────────────
def delivery_callback(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)


def wait_for_kafka(bootstrap_servers: str, timeout: int = 60):
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


def read_all_posts_from_kafka() -> list[dict]:
    """
    Read ALL posts from the reddit.posts Kafka topic (from beginning to end).
    Uses a temporary consumer group that always reads from the start.
    Returns a list of post data dicts.
    """
    logger.info("Reading all posts from Kafka topic '%s'...", KAFKA_TOPIC_POSTS)

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "fetch-comments-reader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_conf)

    # Get topic partitions and assign from beginning
    metadata = consumer.list_topics(topic=KAFKA_TOPIC_POSTS, timeout=10)
    if KAFKA_TOPIC_POSTS not in metadata.topics:
        logger.error("Topic '%s' does not exist!", KAFKA_TOPIC_POSTS)
        consumer.close()
        return []

    partitions = metadata.topics[KAFKA_TOPIC_POSTS].partitions
    tp_list = [TopicPartition(KAFKA_TOPIC_POSTS, p, 0) for p in partitions]
    consumer.assign(tp_list)

    posts = []
    seen_ids = set()
    empty_polls = 0
    max_empty_polls = 10  # Stop after 10 empty polls (= ~5 seconds of no data)

    while empty_polls < max_empty_polls:
        msg = consumer.poll(timeout=0.5)

        if msg is None:
            empty_polls += 1
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
                continue
            logger.error("Consumer error: %s", msg.error())
            continue

        empty_polls = 0  # Reset on successful message

        try:
            data = json.loads(msg.value().decode("utf-8"))
            post_id = data.get("id", "")
            if post_id and post_id not in seen_ids:
                seen_ids.add(post_id)
                posts.append(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning("Failed to decode message: %s", e)

    consumer.close()
    logger.info("Found %d unique posts in Kafka", len(posts))
    return posts


# ── Main logic ──────────────────────────────────────────────────────────────
def fetch_comments_for_post(
    reddit: praw.Reddit,
    producer: Producer,
    post_data: dict,
) -> int:
    """
    Fetch ALL comments for a single post and publish them to Kafka.
    Also publishes updated post metrics (score, upvote_ratio, num_comments)
    to the post-updates topic for ML training targets.
    Returns the number of comments fetched.
    """
    post_id = post_data["id"]
    post_title = post_data.get("title", "")
    post_subreddit = post_data.get("subreddit", "")

    try:
        submission = reddit.submission(id=post_id)
        submission.comments.replace_more(limit=0)  # Expand all comment trees

        # ── Publish updated post metrics ──────────────────────────────
        # The submission now has stable/updated score, ratio, comments
        # since time has passed since the post was first collected.
        try:
            post_update = {
                "id": post_id,
                "original_score": int(post_data.get("score", 0) or 0),
                "original_num_comments": int(post_data.get("num_comments", 0) or 0),
                "original_upvote_ratio": float(post_data.get("upvote_ratio", 0.0) or 0.0),
                "updated_score": submission.score,
                "updated_upvote_ratio": submission.upvote_ratio,
                "updated_num_comments": submission.num_comments,
                "score_growth": submission.score - int(post_data.get("score", 0) or 0),
                "subreddit": post_subreddit,
                "created_utc": post_data.get("created_utc", ""),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "content_type": "post_update",
            }
            update_value = json.dumps(post_update, ensure_ascii=False).encode("utf-8")
            producer.produce(
                KAFKA_TOPIC_POST_UPDATES,
                key=post_id.encode("utf-8"),
                value=update_value,
                callback=delivery_callback,
            )
            producer.poll(0)
            logger.info(
                "  Updated metrics: score %d→%d, comments %d→%d, ratio %.2f→%.2f",
                post_update["original_score"], post_update["updated_score"],
                post_update["original_num_comments"], post_update["updated_num_comments"],
                post_update["original_upvote_ratio"], post_update["updated_upvote_ratio"],
            )
        except Exception as e:
            logger.warning("Failed to publish post update for %s: %s", post_id, e)
        # ──────────────────────────────────────────────────────────────

        comment_count = 0
        for comment in submission.comments.list():
            if should_stop:
                break
            try:
                data = extract_comment_data(
                    comment,
                    post_id=post_id,
                    post_title=post_title,
                    post_subreddit=post_subreddit,
                )
                value = json.dumps(data, ensure_ascii=False).encode("utf-8")
                producer.produce(
                    KAFKA_TOPIC_COMMENTS,
                    key=data["id"].encode("utf-8"),
                    value=value,
                    callback=delivery_callback,
                )
                producer.poll(0)
                comment_count += 1
            except Exception as e:
                logger.error("Error extracting comment %s: %s", getattr(comment, "id", "?"), e)

        return comment_count

    except Exception as e:
        logger.error("Error fetching comments for post %s: %s", post_id, e)
        return 0


def main():
    global should_stop

    logger.info("=" * 60)
    logger.info("  Comment Fetcher — Manual Run")
    logger.info("=" * 60)

    # Validate config
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        logger.critical("REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET must be set!")
        sys.exit(1)

    # Wait for Kafka
    wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    # Load checkpoint (which posts have we already processed?)
    checkpoint = load_checkpoint()
    processed_ids = checkpoint["processed_ids"]

    # Read all posts from Kafka
    all_posts = read_all_posts_from_kafka()

    if not all_posts:
        logger.info("No posts found in Kafka. Run the producer first!")
        return

    # Filter out already-processed posts
    pending_posts = [p for p in all_posts if p["id"] not in processed_ids]

    logger.info("")
    logger.info("  Total posts in Kafka:     %d", len(all_posts))
    logger.info("  Already processed:        %d", len(processed_ids))
    logger.info("  Pending (to fetch now):   %d", len(pending_posts))
    logger.info("")

    if not pending_posts:
        logger.info("All posts already processed! Nothing to do.")
        logger.info("Wait for the producer to collect more posts, then run again.")
        return

    # Create clients
    reddit = create_reddit()
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "fetch-comments",
        "acks": "all",
        "compression.type": "snappy",
        "linger.ms": 50,
    }
    producer = Producer(producer_conf)

    # Process each pending post
    total_comments_this_run = 0
    posts_processed_this_run = 0

    for i, post_data in enumerate(pending_posts):
        if should_stop:
            logger.info("Stop signal received — saving progress...")
            break

        post_id = post_data["id"]
        post_title = post_data.get("title", "")[:60]
        post_subreddit = post_data.get("subreddit", "")

        logger.info(
            "[%d/%d] Fetching comments for: r/%s — %s",
            i + 1, len(pending_posts),
            post_subreddit,
            post_title,
        )

        # Fetch comments for this post
        comment_count = fetch_comments_for_post(reddit, producer, post_data)

        # Mark as processed
        processed_ids.add(post_id)
        total_comments_this_run += comment_count
        posts_processed_this_run += 1
        checkpoint["total_comments"] += comment_count

        logger.info(
            "  → %d comments fetched (total this run: %d)",
            comment_count, total_comments_this_run,
        )

        # Save checkpoint after EACH post (so we don't lose progress)
        save_checkpoint(checkpoint)

        # Respect Reddit API rate limits
        if i < len(pending_posts) - 1 and not should_stop:
            time.sleep(API_DELAY)

    # Final flush
    producer.flush(timeout=30)
    save_checkpoint(checkpoint)

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("  Comment Fetcher — Complete!")
    logger.info("=" * 60)
    logger.info("  Posts processed this run:   %d", posts_processed_this_run)
    logger.info("  Comments fetched this run:  %d", total_comments_this_run)
    logger.info("  Total posts processed:      %d", len(processed_ids))
    logger.info("  Total comments (all runs):  %d", checkpoint["total_comments"])
    logger.info("")
    logger.info("  Checkpoint saved to: %s", CHECKPOINT_FILE)
    logger.info("  Run again later to fetch comments for new posts!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
