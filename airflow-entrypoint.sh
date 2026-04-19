#!/bin/bash

airflow db migrate

echo "Creating admin user for airflow..."

airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin

airflow scheduler &



echo "Starting webserver..."
exec airflow webserver