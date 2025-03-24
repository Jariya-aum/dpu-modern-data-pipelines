import json
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

import requests

DAG_FOLDER = "/opt/airflow/dags"

API_KEY = Variable.get("airvisual_api_key")
CITY = "Bangkok"
STATE = "Bangkok"
COUNTRY = "Thailand"

URL = f"http://api.airvisual.com/v2/city?city={CITY}&state={STATE}&country={COUNTRY}&key={API_KEY}"


def _get_air_quality_data():
    response = requests.get(URL)
    data = response.json()
    
    if data["status"] != "success":
        raise ValueError("Failed to fetch data from API")
    
    with open(f"{DAG_FOLDER}/air_quality.json", "w") as f:
        json.dump(data, f)


def _validate_data():
    with open(f"{DAG_FOLDER}/air_quality.json", "r") as f:
        data = json.load(f)
    
    assert "data" in data, "Missing data field in API response"
    assert "current" in data["data"], "Missing current data"
    assert "pollution" in data["data"]["current"], "Missing pollution data"
    assert "weather" in data["data"]["current"], "Missing weather data"


def _create_air_quality_table():
    pg_hook = PostgresHook(postgres_conn_id="air_quality_postgres_conn", schema="airflow")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    sql = """
    CREATE TABLE IF NOT EXISTS air_quality (
        city TEXT,
        state TEXT,
        country TEXT,
        timestamp TIMESTAMP,
        aqius INT,
        mainus TEXT,
        aqicn INT,
        maincn TEXT,
        temp FLOAT,
        pressure INT,
        humidity INT,
        wind_speed FLOAT,
        wind_direction INT,
        weather_icon TEXT
    )
    """
    cursor.execute(sql)
    connection.commit()


def _load_data_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id="air_quality_postgres_conn", schema="airflow")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    with open(f"{DAG_FOLDER}/air_quality.json", "r") as f:
        data = json.load(f)
    
    pollution = data["data"]["current"]["pollution"]
    weather = data["data"]["current"]["weather"]
    
    sql = """
    INSERT INTO air_quality (
        city, state, country, timestamp, aqius, mainus, aqicn, maincn,
        temp, pressure, humidity, wind_speed, wind_direction, weather_icon
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        data["data"]["city"], data["data"]["state"], data["data"]["country"],
        pollution["ts"], pollution["aqius"], pollution["mainus"], pollution["aqicn"], pollution["maincn"],
        weather["tp"], weather["pr"], weather["hu"], weather["ws"], weather["wd"], weather["ic"]
    )
    cursor.execute(sql, values)
    connection.commit()


default_args = {
    "email": ["kan@odds.team"],
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "air_quality_dag",
    default_args=default_args,
    schedule="0 */3 * * *",
    start_date=timezone.datetime(2025, 2, 1),
    tags=["air_quality"],
):
    start = EmptyOperator(task_id="start")

    get_air_quality_data = PythonOperator(
        task_id="get_air_quality_data",
        python_callable=_get_air_quality_data,
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    create_air_quality_table = PythonOperator(
        task_id="create_air_quality_table",
        python_callable=_create_air_quality_table,
    )

    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    start >> get_air_quality_data >> validate_data >> load_data_to_postgres >> end
    start >> create_air_quality_table >> load_data_to_postgres
