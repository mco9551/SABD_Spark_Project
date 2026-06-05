#!/bin/bash
# ================================================
# SABD Project 1 — Automatic Setup Script
# Usage:
#   ./setup.sh      Run all queries
#   ./setup.sh 1    Run Query 1 only
#   ./setup.sh 2    Run Query 2 only
#   ./setup.sh 3    Run Query 3 only
#   ./setup.sh 4    Run Query 4 only
# ================================================

QUERY_ARG="${1:-0}"

echo "================================================"
echo " SABD Project 1 — Automatic Setup"
if [ "$QUERY_ARG" == "0" ]; then
    echo " Mode: Run all queries"
else
    echo " Mode: Run Query $QUERY_ARG only"
fi
echo "================================================"

echo ""
echo ">>> Step 1 — Starting Docker containers..."
docker compose down 2>/dev/null
docker compose up -d

echo ""
echo ">>> Step 2 — Waiting for containers to start (May takes some time)..."

echo ""
echo ">>> Step 3 — Checking HDFS..."
DATANODES=$(docker exec namenode hdfs dfsadmin -report 2>/dev/null | grep "Live datanodes")
echo "$DATANODES"
if [[ "$DATANODES" == *"(0)"* ]] || [[ -z "$DATANODES" ]]; then
    echo " Datanode not ready yet, waiting 30 more seconds..."
    sleep 30
fi

echo ""
echo ">>> Step 4 — Creating HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /data 2>/dev/null
docker exec namenode hdfs dfs -mkdir -p /results 2>/dev/null

echo ""
echo ">>> Step 5 — Copying data into namenode container..."
docker cp ./data/. namenode:/data/

echo ""
echo ">>> Step 6 — Loading CSV files into HDFS..."
docker exec namenode bash -c "hdfs dfs -put -f /data/*.csv /data/" 2>/dev/null
echo "Files loaded"

echo ""
echo ">>> Step 7 — Verifying data in HDFS..."
docker exec namenode hdfs dfs -ls /data/

echo ""
echo ">>> Step 8 — Maven build..."
mvn clean package -q
if [ $? -ne 0 ]; then
    echo "Maven build failed"
    exit 1
fi
echo "Build successful"
echo ">>> Step 8.5 — Copying JAR to spark-master container..."
docker cp ./target/sabd-project1-1.0-jar-with-dependencies.jar spark-master:/tmp/job.jar


echo ""
echo ">>> Step 9 — Running Spark queries..."
echo "------------------------------------------------"

GLOBAL_START=$(date +%s%3N)

# On ajoute 2>/dev/null à la fin pour avaler les logs Spark (stderr)
# Tes System.out.println s'afficheront toujours !
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --class Main \
    /tmp/job.jar $QUERY_ARG 2>/dev/null

GLOBAL_END=$(date +%s%3N)
GLOBAL_TOTAL=$((GLOBAL_END - GLOBAL_START))

echo "------------------------------------------------"
echo "=== GLOBAL TOTAL TIME : ${GLOBAL_TOTAL} ms"
echo "------------------------------------------------"

echo ""
echo ">>> Step 10 — Retrieving results..."
mkdir -p ./results
docker cp spark-master:/spark/results/. ./results/ 2>/dev/null 1>/dev/null
echo "Results retrieved in ./results/"

echo ""
echo ">>> Step 11 — Generating Python graphs..."
cd graphs
pip install pandas matplotlib scikit-learn -q
python generate_graphs.py
cd ..

echo ""
echo "================================================"
echo " Setup complete!"
echo " Results  : ./results/"
echo " Graphs   : ./graphs/output/"
echo " Spark UI : http://localhost:8080"
echo " HDFS UI  : http://localhost:9870"
echo "================================================"
