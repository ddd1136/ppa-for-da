import os
import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.settings import json

def parse_file(file_path):
    required_fields = [
        "plan_type",
        "direction_id",
        "ns_id",
        "direction_code",
        "direction_name",
        "edu_program_id",
        "edu_program_name",
        "faculty_id",
        "faculty_name",
        "training_period",
        "university_partner",
        "up_country",
        "lang",
        "military_department",
        "total_intensity",
        "ognp_id",
        "ognp_name",
        "selection_year",
    ]

    with open(file_path, 'r') as f:
        json_data = {**json.load(f), 'disciplines_blocks': None}

    print("[UP DESCRIPTION] Parse file:", file_path)

    is_all_required_fields = all([
        field in list(json_data.keys())
        for field in required_fields
    ])

    if not is_all_required_fields:
        print("[UP DESCRIPTION] Error: skipping file, not all required fields", file_path)
        return pd.DataFrame()

    df = pd.DataFrame([json_data])
    df = df.drop(['disciplines_blocks'], axis=1)

    return df


def get_up_description():
    base_path = '/opt/airflow/data'  # Путь, где находятся JSON-файлы внутри контейнера

    for root, _, files in os.walk(base_path):
        for file_name in files:
            if file_name.endswith('.json'):
                file_path = os.path.join(root, file_name)
                print(file_path)
                df = parse_file(file_path)

                if df.empty:
                    continue

                PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows(
                    'stg.up_description', df.values, target_fields=df.columns.tolist(), replace=True,
                    replace_index='id')


with DAG(dag_id='get_up_descriptions', start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), schedule_interval="@monthly",
         catchup=False) as dag:
    t1 = PythonOperator(
        task_id='get_up_descriptions',
        python_callable=get_up_description
    )

t1 
