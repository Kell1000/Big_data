<div align="center">
  <h1>🚀 Real-Time Reddit Data Engineering Pipeline</h1>
  
  <p>
    An end-to-end Big Data pipeline for ingesting, processing, and storing real-time Reddit posts and comments.
    <br />
    <strong>Developed by <a href="https://github.com/BozIAymane">Aymane Bozian</a> & <a href="https://github.com/OussamaHaimour">Oussama Haimour</a></strong>
  </p>
</div>

## 📖 About The Project

This project demonstrates a fully functional **Big Data architecture** capable of handling real-time streaming data from Reddit. The pipeline is designed to collect data using the Reddit API (PRAW), push it through a distributed message broker (Apache Kafka), process and clean the data on the fly (Apache Flink), and finally store the structured data in a distributed file system (HDFS) in Parquet format.

A companion Data Science cluster is also included for advanced analytics and machine learning.

## 🛠️ Built With

* **Orchestration:** Docker & Docker Compose
* **Ingestion:** Python & PRAW (Reddit API)
* **Message Broker:** Apache Kafka & Zookeeper
* **Stream Processing:** Apache Flink
* **Distributed Storage:** HDFS (Hadoop)
* **Data Format:** Parquet + Snappy 
* **Data Science:** PySpark, Jupyter

## 🏗️ Architecture

```text
Reddit API (PRAW)  ──►  Kafka  ──►  Flink Processor  ──►  HDFS (Parquet)
     │                    ▲            (Cleaning)
     │                    │
     │              fetch-comments
     │              (manual run)
     ▼
  Posts (auto)
  Comments (manual)
```

## 🚀 Getting Started

Follow these steps to deploy the data engineering pipeline on your local machine.

### Prerequisites

* [Docker](https://docs.docker.com/get-docker/) installed and running.
* [Docker Compose](https://docs.docker.com/compose/install/) installed.
* Reddit API Credentials (create an app at [Reddit Prefs](https://www.reddit.com/prefs/apps/)).

### Step 1: Configuration

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
2. Open `.env` and fill in your Reddit API credentials:
   ```env
   REDDIT_CLIENT_ID=your_client_id
   REDDIT_CLIENT_SECRET=your_client_secret
   REDDIT_USER_AGENT=your-app-name
   TARGET_SUBREDDITS=all
   ```

### Step 2: Start the Pipeline

Instantiate all services (Zookeeper, Kafka, HDFS, Reddit Producer, Flink, and HDFS Writer) with Docker Compose:

```bash
docker compose up -d --build
```
*Note: The first build might take a few minutes as it downloads the required images.*

Verify all services are running:
```bash
docker compose ps
```

### Step 3: Monitor Data Ingestion

The `reddit-producer` will automatically start fetching posts in real-time. You can watch the logs:

```bash
docker compose logs -f reddit-producer
```

To fetch comments for the collected posts (this is a deliberate manual step to prevent rate limiting), run:
```bash
docker compose run --rm fetch-comments
```

### Step 4: Verify Data in HDFS

You can interact with HDFS to see the stored Parquet files:

```bash
# List all files in the HDFS directory
docker compose exec namenode hdfs dfs -ls -R /data/reddit

# Check total size of the stored data
docker compose exec namenode hdfs dfs -du -h /data/reddit
```

* **HDFS NameNode UI:** [http://localhost:9870](http://localhost:9870)
* **HDFS DataNode UI:** [http://localhost:9864](http://localhost:9864)

### Step 5: Shutting Down

To stop the pipeline while keeping the collected data:
```bash
docker compose down
```

To stop the pipeline and **delete all data**:
```bash
docker compose down -v
```

## 📁 Project Structure

```
reddit/
├── .env.example                  # API credentials template
├── docker-compose.yml            # Services architecture orchestration
├── reddit-producer/              # Real-time post ingestion (Auto)
├── fetch-comments/               # Comment fetching logic (Manual)
├── flink-processor/              # Stream processing and data cleaning
├── hdfs-writer/                  # Writes structured data to HDFS
├── config/hadoop/                # Core Hadoop configs
├── data-science/                 # Analytics and ML pipeline
└── scripts/                      # Startup and health check utilities
```

## 💡 Data Science Cluster (Optional)

Once your Data Engineering cluster is running and data is flowing, you can launch the Data Science environment.

```bash
cd data-science

# If you have an NVIDIA GPU:
docker compose up -d --build

# For CPU only:
docker compose --profile cpu-only up -d --build jupyter-cpu spark-master spark-worker-1 spark-worker-2
```
Access Jupyter Notebooks at `http://localhost:8888` (password/token: `reddit`).

---
**CMC - Big Data Project**
