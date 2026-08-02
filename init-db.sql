CREATE DATABASE airflow OWNER ibrahim;

CREATE DATABASE smartcity OWNER ibrahim;


\c smartcity

CREATE TABLE pipeline_track (
    "source" varchar(10) primary key, 
    "last_processed" timestamp 
)