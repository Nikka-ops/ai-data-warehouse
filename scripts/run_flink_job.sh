#!/usr/bin/env bash
set -e
JAR=${1:-target/flink-jobs-1.0.0.jar}
MODE=${2:-realtime}
echo "Submitting Flink job: $JAR (mode=$MODE)"
curl -X POST http://localhost:8081/jars/upload \
  -H "Expect:" \
  -F "jarfile=@${JAR}"
echo ""
echo "Job submitted. Monitor at http://localhost:8081"
