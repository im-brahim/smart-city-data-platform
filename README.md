# Smart City Data Platform — Spark + Backblaze B2 (Snapshot)

> **This branch is archived on purpose.** It represents a real, working distributed-Spark pipeline reading from and writing to Backblaze B2 via the S3A connector. I'm keeping it exactly as it is instead of merging it into `main`, because the direction the project needs going forward is different from what this branch proves.

## Why this branch exists

I built this to answer one question honestly: can I actually run Spark against cloud object storage, not just on a toy local dataset? Bronze ingestion was already saving hourly weather and traffic JSON to MinIO, then later migrated to Backblaze B2. This branch is where I moved the Silver processing layer to read that data with Spark's native `s3a://` connector instead of looping over files with boto3 — 57 files in, 57 flattened rows out, confirmed working.

It did what I needed it to do. I got hands-on with:
- A real multi-container Spark cluster (master + worker) in Docker Compose
- The S3A connector against a non-AWS S3-compatible provider (Backblaze B2), including endpoint/path-style config that AWS-only tutorials don't cover
- Driver classpath quirks — each container that creates a `SparkSession` needs its own S3A jars, whether that's the master (`spark-submit`) or a Jupyter container running notebooks
- A durable watermark pattern (`pipeline_track` table in PostgreSQL) to avoid reprocessing files across runs, using `StartAfter` on `list_objects_v2` instead of listing the full bucket every time
- The actual operational pain of running a stateful cluster locally — worker re-registration breaking after the master container gets recreated, Docker Desktop silently dropping network connections after the host sleeps, `df.show()` hanging with no clean error because the driver-local metadata calls still return fine even when the network is dead underneath

## Why I'm not continuing on this path

Partway through, I stepped back and did the actual math: this pipeline ingests ~2 API calls an hour, roughly 1,400 files a month. That's nowhere near the scale Spark is built to justify. Everything hard about this branch — the classpath management, the worker registration, the jar compatibility across Spark images — exists *because* Spark assumes a many-worker, large-file world. At this volume none of those problems exist in plain pandas.

I already have a project that proves distributed Spark competence on its own — [`spark-cluster-docker`](https://github.com/ibrahimelaidouni/spark-cluster-docker), a standalone cluster reading real data from B2 at a scale where Spark actually earns its complexity. Smart City doesn't need to prove that twice. What Smart City actually needs is a pipeline that's cheap to run, easy to reason about, and gets out of the way of the part that matters most for this project — the ML layer.

So going forward, `main` and `dev` move to a plain Python (pandas) implementation of Bronze → Silver, orchestrated with GitHub Actions instead of Airflow, with the same watermark logic carried over unchanged. This branch stays as the record of the Spark version and the reasoning behind the switch — not a step backward, a deliberate engineering call made with evidence in hand.

## Architecture (as of this snapshot)

Bronze → Silver → Gold, Spark-based:

| Layer | Tool | Description |
|---|---|---|
| **Bronze** | boto3 → B2 | Hourly JSON from OpenWeatherMap + TomTom, timestamped files |
| **Silver** | Spark (S3A) → B2 | Read via `s3a://`, flatten, validate, save as Parquet |
| **Gold** | PostgreSQL | Deduplicated load for dashboards/ML |
| **Watermark** | PostgreSQL `pipeline_track` | Tracks `last_processed` per source, drives `StartAfter` on listing |

### Tech stack
Apache Spark 3.5.0 (PySpark), Docker Compose, Backblaze B2 (S3A), PostgreSQL 13, custom Spark image (`ibrahimelaidouni/my-custom-spark:3.5.0`) with S3A + PostgreSQL jars pre-baked in.

### Key files
```
jobs/
  process_city_data.py   → Bronze→Silver, Spark read via s3a://
  save_to_db.py          → Silver→Gold, PostgreSQL load
  validate_data.py       → fail-slow data quality checks
  utils/
    connect.py            → SparkSession factory, S3A config
    data_io.py             → watermark get/update, B2 client, Parquet I/O
    config.py               → B2 + DB configuration
docker-compose.yaml        → master, worker, postgres services
init-db.sql                 → pipeline_track table definition
```

## Running this snapshot

```bash
git clone https://github.com/im-brahim/smart-city-data-platform.git
git checkout feature/cloud-storage
docker compose up -d
```

See `.env.example` for required B2 and PostgreSQL credentials.

---

*This branch is frozen for reference. Active development continues on `main`/`dev` with the pandas + GitHub Actions architecture.*