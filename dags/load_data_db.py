from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore

default_args = {
    "owner": "ibrahim",
    "start_date": datetime(2026, 4, 17),
    # "retries": 1,
    # 'retry_delay': timedelta(minutes=1),
}

with DAG(

    dag_id="save_traffic_to_postgres",
    default_args=default_args,
    schedule_interval=None,   # manual for now
    catchup=False,
    tags=["LOADING", "PROCESS TRAFFIC", "DATABASE"]

) as dag:

    save_to_db_task = BashOperator(
        task_id="load_traffic_to_Table",
        bash_command="""
        echo "=== START DEBUG ==="
        docker exec master spark-submit \
        --master spark://master:7077 \
        /opt/spark/jobs/save_to_db.py
        echo "=== END ==="
        """
    )
        # --jars /opt/spark/jars/postgresql-42.6.0.jar \

    save_to_db_task