docker compose up -d --build


docker compose ps


docker compose run --rm fetch-comments








# Watch posts f real-time
docker compose logs -f reddit-producer

# Chouf sh7al d data
docker compose exec namenode hdfs dfs -du -h /data/reddit

# Chouf fichiers
docker compose exec namenode hdfs dfs -ls -R /data/reddit/posts
docker compose exec namenode hdfs dfs -ls -R /data/reddit/images

# Stop kolchi (keep data)
docker compose down

# Stop kolchi (delete data)  
docker compose down -v



# 1. Assure que Data Engineering cluster kaykhdm
cd c:\Users\youss\Desktop\reddit
docker compose ps

# 2. Lance Data Science cluster
cd data-science

# GPU version (ila 3ndek NVIDIA GPU):
docker compose up -d --build

# CPU version (ila ma3ndekch GPU):
docker compose --profile cpu-only up -d --build jupyter-cpu spark-master spark-worker-1 spark-worker-2

# 3. Fth Jupyter
# → http://localhost:8888 (token: reddit)



















# Real-Time Reddit Data Engineering Pipeline

**Authors:** Aymane Bozian, Oussama Haimour


## Architecture

```
Reddit API (PRAW)  ──►  Kafka  ──►  Flink Processor  ──►  HDFS (Parquet)
     │                    ▲            (Cleaning)
     │                    │
     │              fetch-comments
     │              (manual run)
     ▼
  Posts (auto)
  Comments (manual)
```

---

## Step-by-Step Guide

### Step 1 — Verify Docker is installed

```bash
docker --version
docker compose version
```

Make sure both commands return a version number. If not, install Docker first.

---

### Step 2 — Configure Reddit API credentials

Edit the `.env` file in the project root:

```bash
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=your-app-name
TARGET_SUBREDDITS=all
```

To get credentials, create an app at: https://www.reddit.com/prefs/apps/

---

### Step 3 — Build and start all services

```bash
docker compose up -d --build
```

This will build and start: Zookeeper, Kafka, HDFS (NameNode + DataNode), Reddit Producer, Flink Processor, and HDFS Writer.

The first build may take a few minutes to download images and install dependencies.

---

### Step 4 — Verify all services are running

```bash
docker compose ps
```

Wait until all services show as **running** or **healthy**.

---

### Step 5 — Watch the producer collecting posts in real-time

```bash
docker compose logs -f reddit-producer
```

You should see posts being streamed from Reddit. Press `Ctrl+C` to exit the logs (the producer keeps running in the background).

To watch other services:

```bash
docker compose logs -f flink-processor
docker compose logs -f hdfs-writer
```

---

### Step 6 — Let the producer collect posts

Leave the pipeline running. The longer it runs, the more posts are collected.

- A few minutes = tens of posts
- A few hours = hundreds to thousands of posts

---

### Step 7 — Fetch comments for collected posts (manual)

When you are ready, run:

```bash
docker compose run --rm fetch-comments
```

This will:

1. Read all posts collected so far from Kafka
2. Skip any posts already processed in a previous run
3. Fetch ALL comments for each post from the Reddit API
4. Publish comments to Kafka (linked to their parent post)
5. Save progress to a checkpoint file

You can safely press `Ctrl+C` to stop — progress is saved after each post. Run the same command again later to continue where you left off.

---

### Step 8 — Verify Kafka topics

```bash
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

Expected topics:

- `reddit.posts`
- `reddit.comments`
- `reddit.posts.processed`
- `reddit.comments.processed`

---

### Step 9 — Check data stored in HDFS

```bash
# List all files
docker compose exec namenode hdfs dfs -ls -R /data/reddit

# Check total data size
docker compose exec namenode hdfs dfs -du -h /data/reddit
```

---

### Step 10 — Access HDFS Web UI

Open in your browser:

- **HDFS NameNode**: http://localhost:9870
- **HDFS DataNode**: http://localhost:9864

---

### Step 11 — Stop the pipeline

```bash
# Stop all services (data is preserved)
docker compose down

# Stop and delete ALL data (clean reset)
docker compose down -v
```

---

## Quick Reference

```bash
docker compose up -d --build                  # Start everything
docker compose ps                             # Check status
docker compose logs -f reddit-producer        # Watch post collection
docker compose run --rm fetch-comments        # Fetch comments (manual)
docker compose exec namenode hdfs dfs -ls -R /data/reddit   # Check HDFS
docker compose down                           # Stop (keep data)
docker compose down -v                        # Stop (delete data)
```

---

## Project Structure

```
reddit/
├── .env                          # Reddit API credentials
├── docker-compose.yml            # Service orchestration
├── reddit-producer/              # Streams posts (automatic, 24/7)
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

---

## Technologies

| Technology | Role |
|------------|------|
| Docker & Docker Compose | Containerization & orchestration |
| Apache Kafka | Distributed streaming |
| Apache Zookeeper | Kafka coordination |
| HDFS (Hadoop) | Distributed storage |
| Flink Processor (Python) | Stream processing & cleaning |
| PRAW | Reddit API client |
| Parquet + Snappy | Columnar storage format |
| PyArrow & Pandas | Data processing |

---

## Troubleshooting

```bash
# Rebuild a specific service
docker compose build --no-cache reddit-producer

# Restart a service
docker compose restart flink-processor

# Check Kafka consumer lag
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --all-groups

# View HDFS health report
docker compose exec namenode hdfs dfsadmin -report
```
