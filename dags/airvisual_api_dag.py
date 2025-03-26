from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
import requests

def _get_air_quality_data():
    API_KEY = "ca802658-e8b7-4810-91e5-a304faf0f38c"
    city = "Bangkok"
    state = "Bangkok"
    country = "Thailand"
    
    url = f"http://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={API_KEY}"
    response = requests.get(url)
    
    print("Requesting URL:", url)
    print("Response Status Code:", response.status_code)
    print("Response Text:", response.text)

with DAG(
    "airvisual_api_dag",
    schedule_interval="@hourly",  # รันทุกชั่วโมง
    start_date=timezone.datetime(2024, 1, 27),
    catchup=False,
) as dag:
    
    start = EmptyOperator(task_id="start")

    get_air_quality_data = PythonOperator(
        task_id="get_air_quality_data",
        python_callable=_get_air_quality_data,
    )

    end = EmptyOperator(task_id="end")

    start >> get_air_quality_data >> end
