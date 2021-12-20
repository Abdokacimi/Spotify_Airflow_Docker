#! usr/bin/env python3

from airflow import DAG

from datetime import timedelta

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

import ETL_spotify_extract as extract
import ETL_spotify_transform as transform
import ETL_spotify_load as load

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='Spotify_ETL_Job',
    default_args=default_args,
    description='Get recently played tracks from Spotify account history',
    schedule_interval=timedelta(days=7),
)

run_extract = PythonOperator(
    task_id="etl_spotify_extract",
    python_callable=extract.extract_from_spotify,
    dag=dag,
    do_xcom_push=False,
)

run_transform = PythonOperator(
    task_id="etl_spotify_transform",
    python_callable=transform.validate,
    dag=dag,
)

run_load = PythonOperator(
    task_id="etl_spotify_load",
    python_callable=load.load_to_db,
    dag=dag,
)

run_extract >> run_transform >> run_load
