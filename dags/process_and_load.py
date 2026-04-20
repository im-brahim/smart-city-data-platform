from datetime import datetime

from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore

default_args = {
    "owner": "ibrahim",
    "start_date": datetime(2026, 4, 17),
    # "retries": 1,
    # 'retry_delay': '@daily',
}

with DAG(

    dag_id="save_SmartCity_to_postgres",
    default_args=default_args,
    schedule_interval="@daily",   # manual for now
    catchup=False,
    tags=["Processing", "SMART CITY", "LOADING", "DATABASE"]

) as dag:

    task_process = BashOperator(
    task_id="process_city_data",
    bash_command="""
        echo "=== START PROCESS ==="
        docker exec master spark-submit \
        --master spark://master:7077 \
        /opt/spark/jobs/process_city_data.py
        echo "=== END PROCESS ==="
        """
    )

    save_to_db_task = BashOperator(
        task_id="save_to_data",
        bash_command="""
        echo "=== START DEBUG ==="
        docker exec master spark-submit \
        --master spark://master:7077 \
        /opt/spark/jobs/save_to_db.py
        echo "=== END ==="
        """
    )

    task_process >> save_to_db_task