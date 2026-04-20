import json 
from pathlib import Path
import pandas as pd

def run_silver_transform(**context):
    execution_date = context['ds_nodash']
    bronze_file = context['ti'].xcom_pull(key = 'bronze_file_path', task_ids= 'bronze_ingestion')

    if not bronze_file:
        raise ValueError("Bronze file path not found in xcom")

    silver_path = Path("/opt/airflow/data/silver")

    with open(bronze_file) as f:
        raw_data = json.load(f)

    df_raw = pd.DataFrame(raw_data['states'])

    df_raw.columns = ['icao24', 'callsign', 'origin_country', 'time_position', 'last_contact', 
                  'longitude', 'latitude', 'baro_altitude', 'on_ground', 'velocity', 'true_track', 
                  'vertical_rate', 'sensors', 'geo_altitude', 'squawk', 'spi', 'position_source'] 
    
    df = df_raw[
        [
            "icao24",
            "origin_country",
            "velocity",
            "on_ground",
        ]
    ]

    output_file = silver_path / f"flights_{execution_date}.csv"
    df.to_csv(output_file, index=False)

    context['ti'].xcom_push(key = "silver_file", value= str(output_file))
