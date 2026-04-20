# Smart City Data Platform — Casablanca

A production-style data engineering pipeline that collects, processes, and stores real-time weather and traffic data for Casablanca, Morocco. Built to feed a machine learning model for urban traffic prediction — extending my Master's thesis research from historical NYC data to live city data.

## Architecture

Bronze → Silver → Gold lakehouse pattern:
- **Bronze**: Raw JSON files ingested hourly from OpenWeatherMap and TomTom APIs, stored in MinIO
- **Silver**: Spark batch processing — flattening, validation, enrichment — saved as Parquet
- **Gold**: Reading the saved parquet — Deduplicated data loaded into PostgreSQL if Exist, ready for Grafana dashboards and ML models

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
### build the images services:
in the root directory run:
```bash
docker compose up -d
```
*Note:* the spark used is from my docker hub is customized to include the necessary jars for connecting to minio (S3 compatiblity and for Postgres connections)

*Note:* ALL the necessary Scripts Jobs and dags are mounted (local and inside containers) so u can easily modify directly)

Every task is scheduled by airflow dags, however if you need to run a specific job scripts manually : 
```bash
docker exec -it master spark-submit path_to_script # e.g : path_to_script -> /opt/spark/jobs/process_city_data.py
```
or u can use the Git terminal for this shortcut
```bash
./run.sh "file_name.py"
```

### The Structure of the Project:
/SMART-CITY-DATA-PLATFORM:
    /dags
        - ingest_traffic.py     --> ingest traffic api every houre
        - ingest_weather.py     --> ingest weather api every houre
        - process_and_load.py   --> process the ingested data -> Processed theme -> Load to database
    /jobs
    ...



***Important Note:***
the Spark need additionel jars to apple to connect to minio (S3 Compatibility) and Postgres Connections; it's already including in the ccustume spark image i use in this project, if u use an officiel apache spark image or bitnami you need to add these 3 jars to the folder inside the spark service (containers: master and workers) where is located on:



### to Check the services UI: 
Airflow UI: http://localhost:8081/
Spark Master UI: http://localhost:8080/
Spark Worker UI:http://localhost:8082/
Minio UI: http://localhost:9001/

***See .env.example for required credentials***

## What I learned

- the moste important daily git & docker command ... 
- branch main for production and dev for development ...

- the moste architecture pattern that used by the real companies: Bronze -> Silver -> Gold 

- a Validation of the data is more important than ingest and load it 
- A Fail Slow Validation Pattern
- DRY: Don't Reapeat Yourself -> making a task in a function so  will use it whene i need it not to rewrite the logic each time 

- Singelton Pattern : Create an expensive object ONCE, reuse it instead of recreating on every function call.

### code quality and security:
- Any credential or hardcode should past in a secure file and never published (including in .gitignore)
- Spark need Some additional Jars to be apple to connect, read and write with S3 compatibility and Postgres
- Airflow dags Containts many type to excute a task including: pythonOperator , bashOperator ...  
- PEP8 biblio ordering 
- Using Docstring to explain the role args and the return if exist for each function for easy understanding its role.  
- Error Handling: 
    > requests.exceptions.RequestException with an exc_info=True for displying the specific error happened

- the main() pattern to make the code portable and appility to use return to exit cleanly

- Shared Volume between the services so one place for the configurations . 
## What's next

- Grafana dashboard for weather/traffic correlation
- Connect live Casablanca data to NYC traffic model
- Retrain model on Casablanca-specific patterns