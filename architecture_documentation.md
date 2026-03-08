# Real-Time Reddit Data Engineering Pipeline

## Architecture & Technical Documentation

---

## 1. Project Overview

This project implements a **fully containerized, real-time data engineering pipeline** that collects live data from Reddit, processes and cleans it, and stores it in a structured format ready for data science and machine learning.

The entire system runs on **Docker**, requiring just one command to deploy all components.

---

## 2. Architecture Overview

```
                          AUTOMATED (runs 24/7)

  Reddit API  ──►  Kafka (Broker)  ──►  Flink Processor  ──►  HDFS (Parquet)
  (PRAW)           Topics:              - Remove URLs          Structured,
  Streams          - posts              - Strip HTML           partitioned
  new posts        - comments           - Normalize            by date.
  in real-time     - processed          - Validate             Ready for ML.
                       ▲
                       |
                  MANUAL (run when you want)
              ┌──────────────────────┐
              │   Comment Fetcher    │
              │                      │
              │ • Reads collected    │
              │   posts from Kafka   │
              │ • Fetches ALL        │
              │   comments per post  │
              │ • Saves progress     │
              │   (checkpoint)       │
              └──────────────────────┘
```

---

## 3. Components Explained

### 3.1 Reddit Producer (reddit-producer/)

- **What it does**: Connects to the Reddit API using PRAW and streams new posts in real-time from any subreddit (configured via environment variable, default: all of Reddit).
- **Runs**: Automatically, 24/7 in the background.
- **Output**: Publishes each post as a JSON message to a Kafka topic called `reddit.posts`.
- **Data captured per post**: ID, title, body text, author, score, upvote ratio, number of comments, subreddit, timestamp, URL, flairs, awards, NSFW flag, and more.
- **Fault tolerance**: Includes retry logic with exponential backoff for API failures.

### 3.2 Comment Fetcher (fetch-comments/)

- **What it does**: Goes through all the posts already collected and fetches their full comment trees from the Reddit API.
- **Runs**: Manually — you run it whenever you decide the posts have had enough time to accumulate comments.
- **Checkpoint system**: Saves progress to a persistent file (checkpoint.json) stored in a Docker volume. If you interrupt it (Ctrl+C), it remembers exactly where it stopped. On the next run, it only processes new posts that haven't been fetched yet.
- **Output**: Publishes each comment to a Kafka topic called `reddit.comments`, with every comment linked to its parent post via `post_id` and `post_title`.
- **Comment fields include**: body, author, score, post_id, post_title, parent_id, is_top_level, depth, controversiality, subreddit, timestamp, and more.

### 3.3 Apache Kafka (Streaming Backbone)

- **What it does**: Acts as a distributed message queue between all components. Decouples producers from consumers.
- **Topics**:
  - `reddit.posts` — Raw posts from the producer
  - `reddit.comments` — Raw comments from the comment fetcher
  - `reddit.posts.processed` — Cleaned posts (output of Flink)
  - `reddit.comments.processed` — Cleaned comments (output of Flink)
- **Why Kafka?**: Handles high throughput, provides fault tolerance (data is not lost if a consumer goes down), and allows multiple consumers to read the same data independently.
- **Configuration**: 3 partitions per topic, Snappy compression, 7-day retention.

### 3.4 Flink Processor (flink-processor/)

- **What it does**: Consumes raw data from Kafka and applies real-time cleaning and transformation:
  - Removes URLs from text content
  - Decodes HTML entities (e.g., &amp; becomes &)
  - Strips Reddit markdown formatting (bold, italic, links)
  - Normalizes whitespace (removes excessive spaces)
  - Validates and enforces data types (score as integer, edited as boolean)
  - Marks deleted/removed content
  - Normalizes subreddit names to lowercase
- **Runs**: Automatically, processes every message as it arrives in real-time.
- **Output**: Publishes cleaned data to processed Kafka topics.

### 3.5 HDFS Writer (hdfs-writer/)

- **What it does**: Consumes cleaned data from processed Kafka topics and writes it to HDFS as Snappy-compressed Parquet files.
- **Buffering**: Accumulates records in memory (500 records or 60 seconds) before writing a batch, minimizing small file overhead.
- **Partitioning**: Data is organized by year/month/day for efficient querying:
  - `/data/reddit/posts/year=2026/month=02/day=18/batch_xxx.parquet`
  - `/data/reddit/comments/year=2026/month=02/day=18/batch_yyy.parquet`
- **Schema enforcement**: Uses Apache Arrow schemas to ensure type safety in Parquet files.

### 3.6 HDFS (Hadoop Distributed File System)

- **NameNode**: Manages filesystem metadata (index of where every file is stored).
- **DataNode**: Stores the actual data blocks.
- **Web UI**: Accessible at http://localhost:9870 to browse stored files.
- **Configuration**: Single replication (development mode), WebHDFS enabled.

---

## 4. Data Flow — Step by Step

1. **Reddit Producer** streams a new post from Reddit (e.g., r/python).
2. The post is published to the Kafka topic `reddit.posts`.
3. **Flink Processor** picks it up, cleans the text, and validates all fields.
4. The cleaned post is published to `reddit.posts.processed`.
5. **HDFS Writer** picks it up and buffers it with other posts.
6. Once the buffer is full, it writes a Parquet file to HDFS.
7. Later, you manually run the **Comment Fetcher**.
8. The Comment Fetcher reads posts from Kafka, fetches all comments from the Reddit API, and publishes them to `reddit.comments`.
9. The same pipeline applies: Flink cleans the comments, HDFS Writer stores them as Parquet.

---

## 5. Post-Comment Relationship

Every comment is linked to its parent post through these fields:

| Field | Example | Description |
|-------|---------|-------------|
| post_id | abc123 | The ID of the post this comment belongs to |
| post_title | Best tips for Python | Title of the parent post |
| parent_id | t3_abc123 or t1_xyz | Direct parent (post or another comment) |
| is_top_level | true / false | Whether it is a direct reply to the post |
| depth | 0, 1, 2... | Nesting level in the comment tree |

To join posts with their comments for analysis:

```python
import pandas as pd

posts = pd.read_parquet("posts/")
comments = pd.read_parquet("comments/")
full_data = comments.merge(posts, left_on="post_id", right_on="id",
                           suffixes=("_comment", "_post"))
```

---

## 6. Usage Instructions

### Start the Pipeline

```bash
docker compose up -d --build     # Build and start all services
docker compose logs -f           # Watch logs in real-time
docker compose logs -f reddit-producer   # Watch only the producer
```

### Fetch Comments (Manual)

```bash
docker compose run --rm fetch-comments   # Fetch comments for collected posts
```

### Monitor

```bash
docker compose ps                        # Check service status
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
docker compose exec namenode hdfs dfs -ls -R /data/reddit
```

### Stop

```bash
docker compose down      # Stop services (keeps data)
docker compose down -v   # Stop and delete all data
```

---

## 7. Technologies Used

| Technology | Version | Role |
|------------|---------|------|
| Docker and Docker Compose | v2+ | Containerization and orchestration |
| Apache Kafka (Confluent) | 7.7.1 | Distributed streaming platform |
| Apache Zookeeper | 7.7.1 | Kafka cluster coordination |
| HDFS (Hadoop) | 3.2.1 | Distributed file storage |
| Python | 3.12 | Programming language for all services |
| PRAW | 7.8.1 | Reddit API client library |
| Apache Parquet + Snappy | - | Columnar storage format with compression |
| PyArrow | 18.1.0 | Parquet read/write library |
| Pandas | 2.2.3 | Data manipulation |
| confluent-kafka | 2.6.1 | Kafka client library for Python |

---

## 8. Data Schema

### Post Fields

| Field | Type | Description |
|-------|------|-------------|
| id | string | Unique post identifier |
| title | string | Post title (cleaned) |
| selftext | string | Post body text (cleaned) |
| author | string | Author username |
| score | integer | Net upvote score |
| upvote_ratio | float | Ratio of upvotes to total votes |
| num_comments | integer | Total number of comments |
| subreddit | string | Subreddit name |
| created_utc | string | Creation timestamp (ISO-8601 UTC) |
| url | string | Post URL |
| permalink | string | Reddit permalink |
| is_self | boolean | Whether it is a self/text post |
| over_18 | boolean | NSFW flag |
| link_flair_text | string | Post flair |
| total_awards_received | integer | Number of awards |

### Comment Fields

| Field | Type | Description |
|-------|------|-------------|
| id | string | Unique comment identifier |
| body | string | Comment text (cleaned) |
| author | string | Author username |
| score | integer | Net upvote score |
| post_id | string | ID of the parent post |
| post_title | string | Title of the parent post |
| parent_id | string | Direct parent (post or comment) |
| is_top_level | boolean | Direct reply to post |
| depth | integer | Nesting level |
| subreddit | string | Subreddit name |
| created_utc | string | Creation timestamp (ISO-8601 UTC) |
| controversiality | integer | Controversiality score |
| is_deleted | boolean | Whether content was deleted |

---

## 9. HDFS Storage Structure

```
/data/reddit/
├── posts/
│   └── year=2026/
│       └── month=02/
│           └── day=18/
│               ├── batch_abc123_143022.parquet
│               └── batch_def456_144500.parquet
└── comments/
    └── year=2026/
        └── month=02/
            └── day=18/
                └── batch_ghi789_143055.parquet
```

---

## 10. Use Cases for the Stored Data

The cleaned and structured Parquet data stored in HDFS is ready for:

- **Sentiment Analysis** — Classify the tone and emotion of posts and comments.
- **Topic Modeling** — Discover trending topics using LDA or BERTopic.
- **Trend Detection** — Track keyword and subreddit popularity over time.
- **User Behavior Analysis** — Study posting and commenting patterns.
- **Predictive Modeling** — Predict post engagement metrics (score, comment count).
- **Text Classification** — Build classifiers for content categorization.
- **Network Analysis** — Map reply chains and user interaction graphs.

---

## 11. Project Structure

```
reddit/
├── .env                          # Reddit API credentials
├── docker-compose.yml            # Full service orchestration
├── reddit-producer/              # Streams posts (automatic)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer.py
│   └── config.py
├── fetch-comments/               # Fetches comments (manual)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── fetch_comments.py
├── flink-processor/              # Cleans and transforms data
│   ├── Dockerfile
│   ├── requirements.txt
│   └── processor.py
├── hdfs-writer/                  # Writes Parquet to HDFS
│   ├── Dockerfile
│   ├── requirements.txt
│   └── writer.py
├── config/hadoop/                # Hadoop configuration
│   ├── core-site.xml
│   └── hdfs-site.xml
└── scripts/                      # Utility scripts
    ├── init-hdfs.sh
    └── check-health.sh
```
