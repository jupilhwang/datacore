#!/bin/bash
set -e

echo "Deploying DataCore to Production..."
if [ ! -f .env.production ]; then
    echo "Error: .env.production file not found."
    exit 1
fi

docker compose -f docker-compose.production.yml --env-file .env.production pull
docker compose -f docker-compose.production.yml --env-file .env.production up -d

echo "Deployment completed successfully."