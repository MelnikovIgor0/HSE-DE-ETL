from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests

JSON_URL = "https://learnwebcode.github.io/json-example/pets-data.json"
OUTPUT_PATH = "/opt/airflow/data/output/flatten_json.json"

default_args = {
    'owner': 'Melnikov Igor',
    'start_date': datetime(2026, 1, 24)
}

def flatten_json():
    try:
        response = requests.get(JSON_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        result = []
        assert len(list(data.keys())) == 1
        for item in data[list(data.keys())[0]]:
            result.append(item)
        print(json.dumps(result, indent=4))
        with open(OUTPUT_PATH, 'w') as file:
            json.dump(result, file, indent=4)
        return True
    except Exception as e:
        raise

with DAG(
    'flatten_json',
    default_args=default_args,
    description='Flatten JSON to linear',
    schedule_interval=None,
    catchup=False
) as dag:
    flatten_json_task = PythonOperator(
        task_id='flatten_json',
        python_callable=flatten_json,
    )
