#!/bin/bash

# Solar BI Site Fetcher Backfill Script
# Usage: ./run_backfill.sh <start_date> <end_date>
# Example: ./run_backfill.sh 2025-08-01 2025-08-09

if [ $# -ne 2 ]; then
    echo "Usage: $0 <start_date> <end_date>"
    echo "Example: $0 2025-08-09 2025-08-19"
    exit 1
fi

START_DATE=$1
END_DATE=$2

echo "Running Solar BI Site Fetcher backfill for dates: $START_DATE to $END_DATE"

# Run the container with the specified date range
docker run --rm \
    --network telemetry-net \
    -v $(pwd)/secrets.json:/app/secrets.json \
    -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    site_fetcher-producer:latest \
    python app.py --start-date $START_DATE --end-date $END_DATE



