from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os

default_args = {
    'start_date': datetime(2025,8,20),
    'retries': 1,
    'retry_delay' : timedelta(minutes=2)
}

dag = DAG(
    dag_id = 'etl_employees_dag',
    default_args = default_args,
    schedule='@daily',
    catchup = False
)

RAW = "/opt/airflow/data/raw/employees.csv"
PROCESSED = "/opt/airflow/data/processed/employees_cleaned.csv"
DB_PATH = "/opt/airflow/data//sqlite/employees.db"     

def extract():
    print("✅ employees.csv is ready at", RAW)

def transform():
    df = pd.read_csv(RAW)
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")
    df = df[df["salary"].notna()]
    df["bonus"] = (df["salary"] * 0.1).round(2)
    df = df[df["salary"] >= 60000]
    df["department"] = df["department"].str.upper()
    df.to_csv(PROCESSED, index=False)

def load():
    # ensure parent directory exists (safe if it already does)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    df = pd.read_csv(PROCESSED)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("employees", conn, if_exists="replace", index=False)
    conn.close()

# Operators
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

# DAG flow
extract_task >> transform_task >> load_task