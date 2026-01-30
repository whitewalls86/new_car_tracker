# CarTracker

CarTracker is a tool to track local and national new car inventory for particular models. It focuses on getting
accurate, timely data on large numbers of vehicles across a variety of markets in order to find the best deal on a 
particular make or model of new car.

## Architecture Overview

CarTracker is composed of several containerized services orchestrated via Docker Compose:

- **FastAPI Scraper**
  - Fetches search result pages and vehicle detail pages
  - Stores raw HTML artifacts for auditability

- **n8n**
  - Orchestrates scraping, parsing, and refresh workflows
  - Enforces batching, staleness rules, and idempotency

- **PostgreSQL**
  - Stores configuration, audit tables, parsed observations, and derived analytics tables

- **dbt**
  - Transforms raw observations into VIN-centric analytics models
  - Enforces freshness and business logic rules

- **dbt_runner**
  - Lightweight API that allows n8n to trigger dbt builds incrementally

## Data Flow

1. Search configurations define which Cars.com searches to run
2. n8n triggers listing scrapes (national + local)
3. Raw HTML pages are stored in the docker container, with filepaths recorded in ```raw_artifacts```.
4. Parsed search result pages produce `srp_observations`
5. Detail pages produce:
   - `detail_observations`
   - `detail_carousel_hints`
     - This table stores the information about similar vehicles at the same dealer without needed to call new searches.
6. dbt builds VIN-centric models such as:
   - `mart_vehicle_snapshot`
   - `ops_vehicle_staleness`

## Refresh Strategy

- Price data is considered stale after 24 hours
- Full vehicle details are refreshed weekly
- Detail scrapes are limited to 1 listing per dealer to take advantage of carousel listings

## Running Locally

```bash
docker compose up -d
```
### n8n UI
```
http://localhost:5678
```
### Scraper API
```
http://localhost:8000
```
### dbt runner
```
http://localhost:8081
```

---
## Analytics & Reporting

Analytics tables are built using dbt and live in the `models/` directory.
