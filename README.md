# Solar BI - Site Fetcher
### How to use

Create docker network:
```bash
sudo docker network create telemetry-net
```


Run docker container:
```bash
sudo docker compose up --build -d
```

### What is this image responsible for?
The goal of this image is to fetch data reliant on a site id. This data is less likely to change within the timeframe of a day, so it will exit after it completes the fetch. This will allow the container to be deployed at whatever frequency required.

### Running Historical Backfill
To run the site fetcher for historical data, you can specify a date range:

```bash
# Using the convenience script
./run_backfill.sh 2025-08-01 2025-08-09

# Or directly with docker run
docker run --rm \
    --network telemetry-net \
    -v $(pwd)/secrets.json:/app/secrets.json \
    -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    site_fetcher-producer:latest \
    python app.py --start-date 2025-08-01 --end-date 2025-08-09
```

The dates should be in YYYY-MM-DD format. If no dates are provided, it defaults to yesterday's data.


## Site Fetcher
The Site Fetcher is a data collection service developed by Solar BI that:
1. Fetches solar site data from the AlsoEnergy API - a solar monitoring platform
2. Collects site information including site details, hardware configurations, and performance metrics
3. Processes multiple metrics such as:
 - Energy production (Production meter net energy, Inverter net energy)
 - Irradiance data (POA Sensor, POA, Insolation estimate)
 - Performance metrics (Expected energy, Estimated Production)
 - System health (Site uptime, availability, various loss calculations)
4. Publishes data to Kafka for downstream processing in the data pipeline
5. Runs as a scheduled job - it's designed to run daily and exit after completion

## Requires:
Python 3.10.x
Confluent Kafka
Requests 


## Key Features:

*Authentication* - OAuth2 token-based authentication with AlsoEnergy API
*Retry Logic* - Robust HTTP session with automatic retries for failed requests
*Error Handling* - Comprehensive error tracking and logging
*Modular Design* - Separated into:
app.py - Main application logic
solarbi.py - Communication utilities for Kafka messaging
utils.py - Helper functions for HTTP sessions and Kafka callbacks
