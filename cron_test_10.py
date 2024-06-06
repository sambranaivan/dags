from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def execute_stored_procedure():
    # Define the hook to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id='desa_criminis')
    # Execute thess stored procedure
    pg_hook.run("SELECT * FROM cron.test();")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cron_test_10',
    default_args=default_args,
    description='A simple DAG to execute a stored procedure in PostgreSQL',
    schedule_interval=timedelta(days=1),  # Adjust the schedule as needed
    start_date=datetime(2024, 6, 1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='execute_stored_procedure',
    python_callable=execute_stored_procedure,
    dag=dag,
    task_concurrency=1,
)

#execute_sp_task