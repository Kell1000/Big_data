"""
Data Loader — Functions to read data from HDFS into PySpark DataFrames.

Usage:
    from src.data_loader import create_spark_session, load_posts, load_comments, download_images
"""
import os
import json
import logging
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "local[*]")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
HDFS_WEBHDFS = os.getenv("HDFS_WEBHDFS", "http://namenode:9870")

HDFS_POSTS_PATH = f"{HDFS_NAMENODE}/data/reddit/posts"
HDFS_COMMENTS_PATH = f"{HDFS_NAMENODE}/data/reddit/comments"
HDFS_POST_UPDATES_PATH = f"{HDFS_NAMENODE}/data/reddit/post_updates"
HDFS_IMAGES_PATH = "/data/reddit/images"

LOCAL_DATA_DIR = Path("/home/jovyan/data")
LOCAL_IMAGES_DIR = LOCAL_DATA_DIR / "images"


def create_spark_session(app_name: str = "RedditDataScience"):
    """
    Create a PySpark session connected to the Spark cluster and HDFS.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(app_name)
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created: %s", spark.sparkContext.master)
    return spark


def load_posts(spark, path: str = None):
    """
    Load posts from HDFS Parquet files.
    Returns a PySpark DataFrame.
    """
    path = path or HDFS_POSTS_PATH
    logger.info("Loading posts from: %s", path)
    df = spark.read.parquet(path)
    count = df.count()
    logger.info("Loaded %d posts", count)
    return df


def load_comments(spark, path: str = None):
    """
    Load comments from HDFS Parquet files.
    Returns a PySpark DataFrame.
    """
    path = path or HDFS_COMMENTS_PATH
    logger.info("Loading comments from: %s", path)
    df = spark.read.parquet(path)
    count = df.count()
    logger.info("Loaded %d comments", count)
    return df


def load_post_updates(spark, path: str = None):
    """
    Load post updates (updated score/ratio/comments) from HDFS.
    These contain the stable metrics collected by fetch-comments.
    Returns a PySpark DataFrame.
    """
    path = path or HDFS_POST_UPDATES_PATH
    logger.info("Loading post updates from: %s", path)
    df = spark.read.parquet(path)
    count = df.count()
    logger.info("Loaded %d post updates", count)
    return df


def join_posts_with_updates(posts_df, updates_df):
    """
    Join original posts with their updated metrics.
    This gives us the stable score/comments as ML targets.
    """
    updates_selected = updates_df.select(
        "id",
        "updated_score",
        "updated_upvote_ratio",
        "updated_num_comments",
        "score_growth",
    ).withColumnRenamed("id", "update_id")

    joined = posts_df.join(
        updates_selected,
        posts_df["id"] == updates_selected["update_id"],
        how="left",
    ).drop("update_id")

    return joined


def join_posts_comments(posts_df, comments_df):
    """
    Join posts with their comments.
    Returns a joined PySpark DataFrame.
    """
    joined = comments_df.join(
        posts_df.select("id", "title", "score", "subreddit", "media_type", "has_media")
              .withColumnRenamed("id", "p_id")
              .withColumnRenamed("title", "post_title_orig")
              .withColumnRenamed("score", "post_score")
              .withColumnRenamed("subreddit", "post_subreddit")
              .withColumnRenamed("media_type", "post_media_type")
              .withColumnRenamed("has_media", "post_has_media"),
        comments_df["post_id"] == "p_id",
        how="left",
    )
    return joined


def list_hdfs_images():
    """
    List all image files stored in HDFS using WebHDFS API.
    Returns a list of dicts with path and size.
    """
    url = f"{HDFS_WEBHDFS}/webhdfs/v1{HDFS_IMAGES_PATH}?op=LISTSTATUS"
    images = []

    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            logger.warning("Failed to list HDFS images: HTTP %d", resp.status_code)
            return images

        items = resp.json().get("FileStatuses", {}).get("FileStatus", [])

        for item in items:
            if item["type"] == "DIRECTORY":
                # Recurse into date directories
                sub_url = f"{HDFS_WEBHDFS}/webhdfs/v1{HDFS_IMAGES_PATH}/{item['pathSuffix']}?op=LISTSTATUS"
                sub_resp = requests.get(sub_url, timeout=10)
                if sub_resp.status_code == 200:
                    sub_items = sub_resp.json().get("FileStatuses", {}).get("FileStatus", [])
                    for sub in sub_items:
                        if sub["type"] == "FILE":
                            images.append({
                                "name": sub["pathSuffix"],
                                "path": f"{HDFS_IMAGES_PATH}/{item['pathSuffix']}/{sub['pathSuffix']}",
                                "size": sub["length"],
                            })
            elif item["type"] == "FILE":
                images.append({
                    "name": item["pathSuffix"],
                    "path": f"{HDFS_IMAGES_PATH}/{item['pathSuffix']}",
                    "size": item["length"],
                })
    except Exception as e:
        logger.error("Error listing HDFS images: %s", e)

    return images


def download_image_from_hdfs(hdfs_path: str, local_dir: str = None) -> str | None:
    """
    Download a single image from HDFS via WebHDFS.
    Returns the local file path.
    """
    local_dir = local_dir or str(LOCAL_IMAGES_DIR)
    os.makedirs(local_dir, exist_ok=True)

    filename = os.path.basename(hdfs_path)
    local_path = os.path.join(local_dir, filename)

    if os.path.exists(local_path):
        return local_path  # Already downloaded

    try:
        url = f"{HDFS_WEBHDFS}/webhdfs/v1{hdfs_path}?op=OPEN"
        resp = requests.get(url, timeout=30, allow_redirects=True)
        if resp.status_code == 200:
            with open(local_path, "wb") as f:
                f.write(resp.content)
            return local_path
        else:
            logger.warning("Failed to download %s: HTTP %d", hdfs_path, resp.status_code)
            return None
    except Exception as e:
        logger.warning("Error downloading %s: %s", hdfs_path, e)
        return None


def download_all_images(max_images: int = None) -> list[str]:
    """
    Download all images from HDFS to local storage.
    Returns list of local file paths.
    """
    from tqdm import tqdm

    images = list_hdfs_images()
    if max_images:
        images = images[:max_images]

    logger.info("Downloading %d images from HDFS...", len(images))
    local_paths = []

    for img in tqdm(images, desc="Downloading images"):
        path = download_image_from_hdfs(img["path"])
        if path:
            local_paths.append(path)

    logger.info("Downloaded %d images to %s", len(local_paths), LOCAL_IMAGES_DIR)
    return local_paths
