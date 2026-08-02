#!/bin/bash
# Usage:
# ./run.sh your_job.py                        (basic run)
# ./run.sh your_job.py jar             (if job needs jars)

JOB=$1
USE_JARS=$2

JARS_OPTION=""
if [ "$USE_JARS" == "jar" ]; then
  JARS_OPTION="--jars /opt/bitnami/spark/jars/postgresql-42.6.0.jar"
fi

docker exec -it master bash -c "export PYTHONPATH=\$PYTHONPATH:/opt/spark && spark-submit $JARS_OPTION /opt/spark/jobs/$JOB"
