from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import pandas as pd

INPUT_PATH = "/opt/airflow/data/input/IOT-temp.csv"
OUTPUT_PATH = "/opt/airflow/data/"

default_args = {
    'owner': 'Melnikov Igor',
    'start_date': datetime(2026, 2, 2)
}

def load_data(**context):
    df = pd.read_csv(INPUT_PATH)
    context['ti'].xcom_push(key='raw_data', value=df.to_json())

def find_hot_and_cold_days(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='raw_data'))
    df['temp'] = df['temp'].astype(float)
    temp_by_date = df.groupby('noted_date')['temp'].mean().reset_index()
    
    hottest = temp_by_date.nlargest(5, 'temp')
    coldest = temp_by_date.nsmallest(5, 'temp')
    
    print("5 hottest days:")
    print(hottest)
    print("5 coldest days:")
    print(coldest)
    
    context['ti'].xcom_push(key='hottest_days', value=hottest.to_json())
    context['ti'].xcom_push(key='coldest_days', value=coldest.to_json())

    with open(OUTPUT_PATH + 'hottest_days.json', 'w') as file:
        json.dump(hottest.to_json(), file, indent=4)
    with open(OUTPUT_PATH + 'coldest_days.json', 'w') as file:
        json.dump(coldest.to_json(), file, indent=4)

def filter_only_in(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='raw_data'))
    print('df shape before filter: ', df.shape)
    df = df[df['out/in'] == 'In']
    print('df shape after filter: ', df.shape)
    context['ti'].xcom_push(key='filtered_in', value=df.to_json())

def process_date(**context):
    def convert_date(date: str) -> str:
        raw_date = datetime.strptime(date, '%d-%m-%Y %H:%M')
        return raw_date.strftime('%y-%m-%d')

    df = pd.read_json(context['ti'].xcom_pull(key='filtered_in'))
    df['noted_date'] = df['noted_date'].apply(convert_date)
    print('after date processing: ', df.head())
    context['ti'].xcom_push(key='processed_date', value=df.to_json())

def clear_by_quantiles(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='processed_date'))
    print('до отсеивания вехних и нижних 5% температуры: ', df.shape)
    q5 = df['temp'].quantile(0.05)
    q95 = df['temp'].quantile(0.95)
    df = df[(df['temp'] >= q5) & (df['temp'] <= q95)]
    print('после отсеивания вехних и нижних 5% температуры: ', df.shape)
    with open(OUTPUT_PATH + 'fully_processed_dataset.json', 'w') as file:
        json.dump(df.to_json(), file, indent=4)

with DAG(
    'ETL_HW_2',
    default_args=default_args,
    description='ETL_HW_2',
    schedule_interval=None,
    catchup=False
) as dag:
    operator_load_data = PythonOperator(task_id='load_data', python_callable=load_data)
    operator_find_hot_and_cold_days = PythonOperator(task_id='find_hot_and_cold_days', python_callable=find_hot_and_cold_days)
    operator_filter_only_in = PythonOperator(task_id='filter_only_in', python_callable=filter_only_in)
    operator_process_date = PythonOperator(task_id='process_date', python_callable=process_date)
    operator_clear_by_quantiles = PythonOperator(task_id='clear_by_quantiles', python_callable=clear_by_quantiles)
    operator_load_data >> operator_find_hot_and_cold_days >> operator_filter_only_in >> operator_process_date >> operator_clear_by_quantiles
