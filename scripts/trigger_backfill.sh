#!/usr/bin/env bash
set -e
START=${1:-$(date -d '30 days ago' '+%Y-%m-%d')}
END=${2:-$(date '+%Y-%m-%d')}
echo "Triggering Kappa backfill from $START to $END"
docker compose exec flink-job python -c "
import sys; sys.path.insert(0, '.')
from src.streaming.kappa_replay import trigger_replay
trigger_replay('$START', '$END')
"
