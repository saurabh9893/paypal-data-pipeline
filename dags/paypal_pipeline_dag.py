# dags/paypal_pipeline_dag.py
# Airflow DAG - orchestrates the full PayPal pipeline daily

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default args applied to all tasks
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# --- Task functions ---

def ingest_paypal_data(**context):
    """Task 1: Fetch transactions from PayPal → upload to ADLS Bronze"""
    from ingestion.paypal_client import get_transactions
    from ingestion.upload_to_adls import upload_json_to_bronze

    transactions = get_transactions()
    path = upload_json_to_bronze(transactions)
    context["ti"].xcom_push(key="bronze_path", value=path)
    print(f"[DAG] Ingestion complete. Bronze path: {path}")


def transform_bronze_to_silver(**context):
    """Task 2: Clean raw JSON → structured Parquet (Silver layer)"""
    # TODO: Day 6 - implement PySpark transformation
    print("[DAG] Bronze → Silver transformation (coming Day 6)")


def transform_silver_to_gold(**context):
    """Task 3: Aggregate metrics → Gold layer"""
    # TODO: Day 8 - implement aggregations
    print("[DAG] Silver → Gold aggregation (coming Day 8)")


def load_to_azure_sql(**context):
    """Task 4: Load Gold data → Azure SQL"""
    # TODO: Day 9 - implement SQL load
    print("[DAG] Load to Azure SQL (coming Day 9)")


# --- DAG Definition ---
with DAG(
    dag_id="paypal_data_pipeline",
    default_args=default_args,
    description="Daily PayPal transaction pipeline",
    schedule_interval="0 6 * * *",  # Every day at 6am UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["paypal", "azure", "data-engineering"],
) as dag:

    t1_ingest = PythonOperator(task_id="ingest_paypal_data",     python_callable=ingest_paypal_data)
    t2_silver = PythonOperator(task_id="bronze_to_silver",       python_callable=transform_bronze_to_silver)
    t3_gold   = PythonOperator(task_id="silver_to_gold",         python_callable=transform_silver_to_gold)
    t4_sql    = PythonOperator(task_id="load_to_azure_sql",      python_callable=load_to_azure_sql)

    # Pipeline order
    t1_ingest >> t2_silver >> t3_gold >> t4_sql
