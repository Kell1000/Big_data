#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# init-hdfs.sh — Initialize HDFS directory structure for the Reddit pipeline
# ─────────────────────────────────────────────────────────────────────────────
set -e

echo "Waiting for HDFS to be available..."
until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is OFF"; do
    echo "  HDFS not ready yet, waiting..."
    sleep 5
done

echo "HDFS is ready. Creating directory structure..."

# Create base directories
hdfs dfs -mkdir -p /data/reddit/posts
hdfs dfs -mkdir -p /data/reddit/comments

# Set permissions (open for all in dev)
hdfs dfs -chmod -R 777 /data

echo "HDFS directory structure created:"
hdfs dfs -ls -R /data/reddit

echo "Done!"
