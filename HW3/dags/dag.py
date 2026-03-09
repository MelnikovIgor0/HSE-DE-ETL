from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

TRANSFORMED_DATA_PATH = '/opt/airflow/data/fully_processed_dataset.json'
HISTORICAL_LOAD_PATH = '/opt/airflow/data/fully_processed_dataset_2.json'
INCREMENTAL_LOAD_PATH = '/opt/airflow/data/incremental_load.json'

def load_historical_data(**context):
    df = pd.read_json(TRANSFORMED_DATA_PATH, lines=True)
    df.to_json(HISTORICAL_LOAD_PATH)
    context['ti'].xcom_push(key='raw_data', value=df.to_json())
    print(f"Historical load completed: {len(df)} records loaded")

def load_incremental_data(**context):
    df = pd.read_json(TRANSFORMED_DATA_PATH, lines=True)
    cutoff_date = datetime.now() - timedelta(days=3)
    df_incremental = df[df['noted_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d')) >= cutoff_date]
    df_incremental.to_json(INCREMENTAL_LOAD_PATH)
    context['ti'].xcom_push('incremental_data', value=df_incremental.to_json())
    print(f"Incremental load completed: {len(df_incremental)} records loaded")

default_args = {
    'owner': 'Melnikov Igor',
    'start_date': datetime(2026, 2, 10),
}

with DAG(
    'hw4_historical_load',
    default_args=default_args,
    description='Историческая загрузка',
    schedule_interval='@once',
) as dag1:
    load_historical = PythonOperator(
        task_id='load_historical_data',
        python_callable=load_historical_data,
    )

with DAG(
    'hw4_incremental_load',
    default_args=default_args,
    description='Инкрементальная загрузка',
    schedule_interval='@daily',
) as dag2:
    load_incremental = PythonOperator(
        task_id='load_incremental_data',
        python_callable=load_incremental_data,
        provide_context=True,
    )
