# Smart City Data Platform — Casablanca

A production-style data engineering pipeline that collects, processes, and stores real-time weather and traffic data for Casablanca, Morocco. Built to feed a machine learning model for urban traffic prediction — extending my Master's thesis research from historical NYC data to live city data.

## Architecture

Bronze → Silver → Gold lakehouse pattern:
- **Bronze**: Raw JSON files ingested hourly from OpenWeatherMap and TomTom APIs, stored in MinIO
- **Silver**: Spark batch processing — flattening, validation, enrichment — saved as Parquet
- **Gold**: Deduplicated data loaded into PostgreSQL, ready for Grafana dashboards and ML models

## Tech stack

| Tool | Role |
|---|---|
| Apache Airflow 2.7 | Pipeline orchestration |
| Apache Spark 3.5 | Distributed batch processing |
| MinIO | S3-compatible object storage |
| PostgreSQL 13 | Serving layer |
| Docker Compose | Local infrastructure |

## How to run

Clone this repository:
```bash
git clone https://github.com/im-brahim/smart-city-data-platform.git
```

### Build the services:
In the root directory run:
```bash
docker compose up -d
```

> **Note:** The Spark image used is a custom image from my Docker Hub that includes the necessary jars for MinIO (S3 compatibility) and PostgreSQL connections. See the Spark section in "What I Learned" for why.

> **Note:** All job scripts and DAGs are mounted as volumes, so you can modify them directly without rebuilding the containers.

Every task is scheduled by Airflow DAGs. To run a specific job manually:
```bash
./run.sh "file_name.py"
```

### Project structure:
```
/smart-city-data-platform
    /dags
        ingest_traffic.py       → ingest traffic API every hour
        ingest_weather.py       → ingest weather API every hour
        process_and_load.py     → process ingested data → load to database
        utils.py
        requirements.txt
    /jobs
        /utils
            __init__.py
            config.py
            connect.py
            data_io.py
            requirements.txt
        process_city_data.py
        save_to_db.py
        validate_data.py
    docker-compose.yaml
    init-db.sql
    run.sh
    airflow-entrypoint.sh
```

> **Important:** Spark needs additional jars to connect to MinIO (S3 compatibility) and PostgreSQL. These are already included in the custom Spark image. If you use the official Apache or Bitnami image instead, you will need to add the jars manually to `/opt/spark/jars` inside the Spark containers.

### Service UIs:
- Airflow: http://localhost:8081/
- Spark Master: http://localhost:8080/
- Spark Worker: http://localhost:8082/
- MinIO: http://localhost:9001/

*See `.env.example` for required credentials.*

---

## What I Learned

### Data pipeline design

The pipeline ingests hourly weather and traffic data from OpenWeatherMap and TomTom APIs. Before saving anything, I add an `ingested_at` field to each API response — the exact UTC time the request was made. This field becomes the deduplication key later in the Gold layer.

One thing I had to figure out early was how to store data that arrives every hour. My first idea was to append each response to a single JSON file, but that doesn't work well — Spark isn't designed to read one large appended file, and concurrent writes risk corrupting it. So I changed the approach: each API call saves a separate file in MinIO with a timestamped name like `2026-04-17T10-00-00.json`. Spark then reads the entire folder in one shot and processes everything together.

One small but important detail — before uploading to MinIO, the API response needs to be converted to bytes:
```python
json_bytes = json.dumps(data).encode('utf-8')
```
MinIO's `put_object()` expects bytes, not a Python dictionary. That took me a moment to figure out the first time.

### Code quality

Early on I had the same utility functions copy-pasted across multiple files — the MinIO upload logic, the logger setup. When I found a bug I had to fix it in three places. I moved everything into shared modules (`dags/utils.py` and `jobs/utils/data_io.py`) so any fix happens once.

For data validation I used the **Fail Slow pattern** — instead of stopping at the first problem, the validator checks all columns and collects every issue before returning. That way I see everything that's wrong in one pipeline run, not one problem per day.

### Infrastructure and Spark

All services run locally using Docker Compose — Airflow, Spark, MinIO, and PostgreSQL on the same network. Using Spark for hourly small batches is not the most efficient choice — pandas would be enough for this data volume. But the goal was to work with the same tools used in real production pipelines, and to prepare for when the historical traffic volume data gets connected later.

For the Spark image, I first tried the official Apache and Bitnami images for version 3.5.0 — the version I had experience with from a previous project. Both had issues with missing or incompatible jars that I couldn't easily fix. Then I remembered I had already built a custom Spark image on my Docker Hub for a previous project, and that image had all the necessary jars included. I switched to it and everything worked. The project is now more portable — no need to manually mount jars or pass `--jars` flags.

### Security and Git

All credentials and API keys stay in a `.env` file listed in `.gitignore` — never committed. I keep a `.env.example` in the repo so anyone cloning knows exactly what variables to set.

For Git I use two branches: `dev` for daily work and `main` only for stable, tested code. This way I never break the working pipeline while adding new features.

---

## What's next

- Grafana dashboard for weather/traffic correlation
- Connect live Casablanca data to NYC traffic model
- Retrain model on Casablanca-specific patterns
