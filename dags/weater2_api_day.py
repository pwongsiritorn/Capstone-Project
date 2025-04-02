import json
import requests
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

# --------- CONFIG ---------
API_KEY = Variable.get("airvisual_api_key")
CITY = "Bangkok"
STATE = "Bangkok"
COUNTRY = "Thailand"
DAG_FOLDER = "/opt/airflow/dags"
DATA_FILE = f"{DAG_FOLDER}/aqi_data1.json"

# --------- TASK 1 Extract ---------
def extract_aqi():
    url = f"https://api.airvisual.com/v2/city?city={CITY}&state={STATE}&country={COUNTRY}&key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    print("API Response:", data)
    with open(DATA_FILE, "w") as f:
        json.dump(data, f)

# --------- TASK 2 Validate ---------
def validate_aqi_data():
    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    pollution = data.get("data", {}).get("current", {}).get("pollution", {})
    assert pollution, "Pollution data missing"
    assert pollution.get("aqius") >= 0, "AQI must be >= 0"

# --------- TASK 3 Transform ---------
def transform_aqi_data():
    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    pollution = data["data"]["current"]["pollution"]
    weather = data["data"]["current"]["weather"]

    transformed = {
        "timestamp": pollution["ts"],
        "aqi": pollution["aqius"],
        "temperature": weather["tp"],
        "humidity": weather["hu"],
        #"pm2_5": pollution.get("pm25", None)
        #"pm2_5": pollution.get("p2", None)
    }

    with open(DATA_FILE, "w") as f:
        json.dump(transformed, f)
    print("Transformed Data:", transformed)

# --------- TASK 4 Create Table ---------
def create_aqi_table():
    pg_hook = PostgresHook(postgres_conn_id="weather2_postgres_conn", schema="postgres")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bangkok_aqi (
            timestamp TIMESTAMP PRIMARY KEY,
            aqi INTEGER NOT NULL,
            temperature FLOAT,
            humidity FLOAT
        )
    """)
    connection.commit()
# --------- TASK 5 Load ---------
def load_to_postgres():
    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    pg_hook = PostgresHook(postgres_conn_id="weather2_postgres_conn", schema="postgres")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        INSERT INTO bangkok_aqi (timestamp, aqi, temperature, humidity)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (timestamp) DO NOTHING
    """
    cursor.execute(sql, (
        data["timestamp"],
        data["aqi"],
        data["temperature"],
        data["humidity"]     
    ))
    connection.commit()

# --------- DAG ---------
with DAG(
    "weater2_api_day",  # 
    start_date=timezone.datetime(2025, 2, 1),
    schedule="0 */3 * * *",   
    catchup=False,
    tags=["dpu", "capstone", "aqi"],
) as dag:

    start = EmptyOperator(task_id="start")
    t1 = PythonOperator(task_id="extract_aqi", python_callable=extract_aqi)
    t2 = PythonOperator(task_id="validate_aqi_data", python_callable=validate_aqi_data)
    t3 = PythonOperator(task_id="transform_aqi_data", python_callable=transform_aqi_data)
    t4 = PythonOperator(task_id="create_aqi_table", python_callable=create_aqi_table)
    t5 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> t3 >> t4 >> t5 >> end

    