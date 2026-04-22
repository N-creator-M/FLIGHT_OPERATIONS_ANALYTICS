import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator # we need this operator to run python in our dag file


AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))


from scripts.bronze_ingest import run_bronze_ingest
from scripts.silver_transform import run_silver_transform
from scripts.golde_aggregate import run_gold_aggregate
from scripts.load_gold_to_snowflake import run_gold_to_snowflake

default_args = {
    'owner': 'airflow',
    "retries" : 0, # if a task fails, how many times should it retry
    "retry_delay" : timedelta(minutes=5) # if a task fails, how long to wait before retrying

}

with DAG(
    dag_id = "flights_ops_medallion_pipe",
    default_args = default_args,
    schedule_interval = "*/30 * * * *", # this means the DAG will run every 30 minutes
    start_date = datetime(2024, 6, 1), # when to start running the DAG
    catchup = False # if True, Airflow will try to "catch up" on all the missed runs since the start date
) as dag:

    bronze = PythonOperator(
        task_id = "bronze_ingest",
        python_callable = run_bronze_ingest
    )
    silver = PythonOperator(
        task_id = "silver_transform",
        python_callable = run_silver_transform
    )

    gold = PythonOperator(
        task_id = "gold_file",
        python_callable = run_gold_aggregate
    )
    load_to_snowflake = PythonOperator(
        task_id = "load_gold_to_snowflake",
        python_callable = run_gold_to_snowflake
    )

    
    bronze >> silver >> gold >> load_to_snowflake # this means that the gold task will run after the silver task is completed