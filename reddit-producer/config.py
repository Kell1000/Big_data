"""
Configuration for the Reddit Producer.
Reads from environment variables with sensible defaults.
"""
import os


# ── Reddit API ──────────────────────────────────────────────────────────────
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "reddit-pipeline-producer")

# ── Kafka ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_POSTS = os.getenv("KAFKA_TOPIC_POSTS", "reddit.posts")

# ── Subreddit config ────────────────────────────────────────────────────────
# Comma-separated list of subreddits to stream from.
# Use "all" for the full firehose.
TARGET_SUBREDDITS = os.getenv("TARGET_SUBREDDITS", "all")

# ── Retry / resilience ──────────────────────────────────────────────────────
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "2.0"))
RETRY_BACKOFF_MAX = float(os.getenv("RETRY_BACKOFF_MAX", "300.0"))
