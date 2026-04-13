import requests, json
from datetime import datetime
from pathlib import Path


URL = "https://opensky-network.org/api/states/all"

def run_bronze_ingestion(**context):
    ti = context['ti']
    response = requests.get(URL, timeout= 30)
    response.raise_for_status()

    data = response.json()

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")

    with open(path, 'w') as file:
        json.dump(data, file)

    ti.xcom_push(key= 'bronze_file_path', value=str(path))