# Solar BI - Site Fetcher
### How to use

Create docker network:
```bash
docker network create telemetry-net
```


Run docker container:
```bash
docker compose up --build -d
```

### What is this image responsible for?
The goal of this image is to fetch data reliant on a site id. This data is less likely to change within the timeframe of a day, so it will exit after it completes the fetch. This will allow the container to be deployed at whatever frequency required.
