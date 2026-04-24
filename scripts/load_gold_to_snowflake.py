import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook


def run_load_gold_to_snowflake(**context):
    gold_file = context['ti'].xcom_pull(key = "gold_file", task_ids = "gold_aggregate")

    if not gold_file:
        raise ValueError("Gold file path not found in xcom")
    
    execution_date = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S")

    df = pd.read_csv(gold_file)

    # Connect to Snowflake
    conn = BaseHook.get_connection("flight_snowflake")

    sf_conn = snowflake.connector.connect(
        user = conn.login,
        password = conn.password,
        account = conn.extra_dejson['account'],
        warehouse = conn.extra_dejson.get('warehouse'),
        database = conn.extra_dejson.get('database'),
        schema = conn.schema,
        role = conn.extra_dejson.get('role')
    )


    merge_sql = """
        MERGE INTO FLIGHT_KPIS tgt
        USING (
            SELECT
                TO_TIMESTAMP(%s) AS WINDOW_START,
                %s AS origin_country,
                %s AS total_flights,
                %s AS avg_velocity,
                %s AS on_ground
        ) src
        ON tgt.WINDOW_START = src.WINDOW_START
           AND tgt.origin_country = src.origin_country
        WHEN MATCHED THEN UPDATE SET
            total_flights = src.total_flights,
            avg_velocity = src.avg_velocity,
            on_ground = src.on_ground,
            load_time = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
        (WINDOW_START, origin_country, total_flights, avg_velocity, on_ground)
        VALUES
        (src.WINDOW_START, src.origin_country, src.total_flights, src.avg_velocity, src.on_ground);
    """


    with sf_conn.cursor() as cursor:
        for _, row in df.iterrows():
            cursor.execute(merge_sql, (
                execution_date,
                row['origin_country'],
                int(row['total_flights']),
                float(row['avg_velocity']),
                int(row['on_ground'])
            )) 

    sf_conn.close()
