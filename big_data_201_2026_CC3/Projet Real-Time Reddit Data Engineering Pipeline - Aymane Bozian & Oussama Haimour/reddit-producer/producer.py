"""
Reddit Producer — Streams NEW posts from Reddit into Kafka.

Architecture:
    Reddit API (stream.submissions) ──▶ Kafka (reddit.posts)

This service ONLY collects posts. Comments are fetched separately
using the fetch_comments.py script (run manually when needed).
"""
import json
import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone

import praw
from confluent_kafka import Producer

import config

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("reddit-producer")

# ── Graceful shutdown ───────────────────────────────────────────────────────
shutdown_event = threading.Event()


def _handle_signal(signum, frame):
    logger.info("Shutdown signal received, stopping stream...")
    shutdown_event.set()


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Kafka helpers ───────────────────────────────────────────────────────────
def create_producer() -> Producer:
    """Create a confluent-kafka Producer with retry-friendly settings."""
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "reddit-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "linger.ms": 100,
        "batch.num.messages": 500,
        "compression.type": "snappy",
        "queue.buffering.max.messages": 100000,
    }
    return Producer(conf)


def delivery_callback(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.debug(
            "Message delivered to %s [%d] @ offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


# ── Reddit helpers ──────────────────────────────────────────────────────────
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')
IMAGE_DOMAINS = ('i.redd.it', 'i.imgur.com', 'preview.redd.it')


def create_reddit() -> praw.Reddit:
    """Create and return an authenticated PRAW Reddit instance."""
    reddit = praw.Reddit(
        client_id=config.REDDIT_CLIENT_ID,
        client_secret=config.REDDIT_CLIENT_SECRET,
        user_agent=config.REDDIT_USER_AGENT,
    )
    reddit.read_only = True
    logger.info("Reddit instance created (read-only mode)")
    return reddit


def extract_image_urls(post) -> list[str]:
    """
    Extract all image URLs from a Reddit post.
    Handles: direct image links, i.redd.it, imgur, galleries, and previews.
    """
    image_urls = []
    seen = set()

    def add_url(url: str):
        if url and url not in seen:
            seen.add(url)
            image_urls.append(url)

    url = getattr(post, 'url', '') or ''

    # 1. Direct image URL (ends with image extension)
    if url.lower().endswith(IMAGE_EXTENSIONS):
        add_url(url)

    # 2. Known image hosting domains
    elif any(domain in url for domain in IMAGE_DOMAINS):
        add_url(url)

    # 3. Gallery posts (multiple images)
    if getattr(post, 'is_gallery', False):
        media_metadata = getattr(post, 'media_metadata', None)
        if media_metadata:
            for item_id, item in media_metadata.items():
                if item.get('status') == 'valid' and 's' in item:
                    img_url = item['s'].get('u', '') or item['s'].get('gif', '')
                    if img_url:
                        # Reddit encodes & as &amp; in gallery URLs
                        img_url = img_url.replace('&amp;', '&')
                        add_url(img_url)

    # 4. Preview images (fallback if no direct image found)
    if not image_urls:
        try:
            preview = getattr(post, 'preview', None)
            if preview and 'images' in preview:
                for img in preview['images']:
                    source = img.get('source', {})
                    if source.get('url'):
                        img_url = source['url'].replace('&amp;', '&')
                        add_url(img_url)
        except (AttributeError, TypeError):
            pass

    return image_urls


def detect_media_type(post, image_urls: list[str]) -> str:
    """
    Determine the media type of a post.
    Returns one of: 'text', 'image', 'gallery', 'video', 'link'
    """
    if getattr(post, 'is_gallery', False):
        return 'gallery'
    if getattr(post, 'is_video', False):
        return 'video'
    if image_urls:
        return 'image'
    if post.is_self:
        return 'text'
    return 'link'


def extract_post_data(post) -> dict:
    """Extract relevant fields from a Reddit submission, including image URLs."""
    image_urls = extract_image_urls(post)
    media_type = detect_media_type(post, image_urls)

    return {
        "id": post.id,
        "title": post.title,
        "selftext": post.selftext or "",
        "author": str(post.author) if post.author else "[deleted]",
        "score": post.score,
        "upvote_ratio": post.upvote_ratio,
        "num_comments": post.num_comments,
        "subreddit": str(post.subreddit),
        "subreddit_id": post.subreddit_id,
        "created_utc": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
        "url": post.url,
        "permalink": f"https://reddit.com{post.permalink}",
        "domain": post.domain,
        "is_self": post.is_self,
        "is_video": post.is_video,
        "over_18": post.over_18,
        "spoiler": post.spoiler,
        "stickied": post.stickied,
        "link_flair_text": post.link_flair_text,
        "author_flair_text": post.author_flair_text,
        "total_awards_received": post.total_awards_received,
        "gilded": post.gilded,
        "distinguished": post.distinguished,
        "edited": bool(post.edited),
        "locked": post.locked,
        "archived": post.archived,
        # ── New image/media fields ──
        "image_urls": image_urls,
        "image_count": len(image_urls),
        "media_type": media_type,
        "has_media": len(image_urls) > 0,
        # ────────────────────────────
        "content_type": "post",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Stream worker ───────────────────────────────────────────────────────────
def stream_posts(reddit: praw.Reddit, producer: Producer):
    """Continuously stream posts from target subreddits into Kafka."""
    topic = config.KAFKA_TOPIC_POSTS
    subreddits = config.TARGET_SUBREDDITS
    retries = 0
    post_count = 0

    while not shutdown_event.is_set():
        try:
            logger.info("Starting post stream on r/%s → topic '%s'", subreddits, topic)
            for post in reddit.subreddit(subreddits).stream.submissions(skip_existing=True):
                if shutdown_event.is_set():
                    break
                try:
                    data = extract_post_data(post)
                    value = json.dumps(data, ensure_ascii=False).encode("utf-8")
                    producer.produce(
                        topic,
                        key=data["id"].encode("utf-8"),
                        value=value,
                        callback=delivery_callback,
                    )
                    producer.poll(0)
                    retries = 0
                    post_count += 1
                    logger.info(
                        "[POST #%d] r/%s — %s (score=%d, comments=%d)",
                        post_count,
                        data["subreddit"],
                        data["title"][:80],
                        data["score"],
                        data["num_comments"],
                    )
                except Exception as e:
                    logger.error("Error processing post %s: %s", getattr(post, "id", "?"), e)

        except Exception as e:
            retries += 1
            backoff = min(config.RETRY_BACKOFF_BASE ** retries, config.RETRY_BACKOFF_MAX)
            logger.error(
                "Post stream error (retry %d/%d, backoff %.0fs): %s",
                retries, config.MAX_RETRIES, backoff, e,
            )
            if retries >= config.MAX_RETRIES:
                logger.critical("Max retries reached for post stream — exiting")
                shutdown_event.set()
                break
            shutdown_event.wait(timeout=backoff)

    producer.flush(timeout=10)
    logger.info("Post stream stopped. Total posts collected: %d", post_count)


# ── Wait for Kafka ──────────────────────────────────────────────────────────
def wait_for_kafka(bootstrap_servers: str, timeout: int = 120):
    """Block until Kafka is reachable or timeout expires."""
    logger.info("Waiting for Kafka at %s ...", bootstrap_servers)
    start = time.time()
    while time.time() - start < timeout:
        try:
            p = Producer({"bootstrap.servers": bootstrap_servers})
            metadata = p.list_topics(timeout=5)
            if metadata.topics is not None:
                logger.info("Kafka is ready (%d topics found)", len(metadata.topics))
                return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError(f"Kafka not reachable at {bootstrap_servers} after {timeout}s")


# ── Entrypoint ──────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  Reddit Producer — Posts Only")
    logger.info("  Target subreddits: %s", config.TARGET_SUBREDDITS)
    logger.info("=" * 60)
    logger.info("")
    logger.info("  NOTE: To fetch comments for collected posts, run:")
    logger.info("    docker compose run --rm fetch-comments")
    logger.info("")

    # Validate mandatory config
    if not config.REDDIT_CLIENT_ID or not config.REDDIT_CLIENT_SECRET:
        logger.critical("REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET must be set!")
        sys.exit(1)

    # Wait for Kafka
    wait_for_kafka(config.KAFKA_BOOTSTRAP_SERVERS)

    # Create clients
    reddit = create_reddit()
    producer = create_producer()

    # Start streaming
    try:
        stream_posts(reddit, producer)
    except KeyboardInterrupt:
        shutdown_event.set()

    logger.info("Reddit Producer shut down cleanly.")


if __name__ == "__main__":
    main()
