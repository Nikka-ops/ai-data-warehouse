#!/usr/bin/env bash
set -e
[ -f .env ] || cp .env.example .env
docker compose --env-file .env up -d
echo "All services started. Dashboard: http://localhost"
