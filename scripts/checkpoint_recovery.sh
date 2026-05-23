#!/usr/bin/env bash
set -e
SAVEPOINT=${1:?"Usage: $0 <savepoint-path>"}
echo "Restoring from savepoint: $SAVEPOINT"
curl -X POST "http://localhost:8081/jobs/${JOB_ID}/savepoints" \
  -H "Content-Type: application/json" \
  -d "{\"target-directory\": \"$SAVEPOINT\", \"cancel-job\": false}"
