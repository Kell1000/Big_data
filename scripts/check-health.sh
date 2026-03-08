#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# check-health.sh — Health check for all pipeline components
# ─────────────────────────────────────────────────────────────────────────────
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "  Reddit Pipeline — Health Check"
echo "=============================================="
echo ""

# Check Docker services
echo "── Docker Services ──"
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
echo ""

# Check Kafka topics
echo "── Kafka Topics ──"
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || echo -e "${RED}Kafka not reachable${NC}"
echo ""

# Check HDFS
echo "── HDFS Status ──"
docker compose exec namenode hdfs dfsadmin -report 2>/dev/null | head -20 || echo -e "${RED}HDFS not reachable${NC}"
echo ""

# Check HDFS data
echo "── HDFS Data ──"
docker compose exec namenode hdfs dfs -ls -R /data/reddit 2>/dev/null || echo -e "${YELLOW}No data in HDFS yet${NC}"
echo ""

echo "=============================================="
echo "  Health check complete"
echo "=============================================="
