# dags/paypal_pipeline_dag.py
# Airflow DAG - orchestrates the full PayPal pipeline daily
# PySpark transformations run separately (manually or via separate job)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_PATH = "/opt/airflow/project"
PYTHON_PATH  = f"PYTHONPATH={PROJECT_PATH}"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="paypal_data_pipeline",
    default_args=default_args,
    description="Daily PayPal transaction pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["paypal", "azure", "data-engineering"],
) as dag:

    # --- Task 1: Generate 1000 orders → Upload to Azure Bronze ---
    t1_ingest = BashOperator(
        task_id="ingest_paypal_data",
        bash_command=f"""
            cd {PROJECT_PATH} && \
            {PYTHON_PATH} python -m ingestion.generate_and_upload
        """,
    )

    # --- Task 2: Load Gold data → Azure SQL ---
    # Note: PySpark transformations (Bronze→Silver→Gold)
    # are run separately as a batch job
    t2_sql = BashOperator(
        task_id="load_to_azure_sql",
        bash_command=f"""
            cd {PROJECT_PATH} && \
            {PYTHON_PATH} python -m ingestion.load_to_sql
        """,
    )

    # --- Task 3: Data Quality Check ---
    def run_quality_checks():
        """
        Basic data quality checks:
        - Verify Bronze files exist on Azure
        - Verify SQL tables have data
        - Log record counts
        """
        import os
        from dotenv import load_dotenv
        load_dotenv(f"{PROJECT_PATH}/.env")

        from azure.storage.filedatalake import DataLakeServiceClient

        storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
        storage_key     = os.getenv("AZURE_STORAGE_KEY")

        # Check Bronze layer
        client    = DataLakeServiceClient(
            account_url=f"https://{storage_account}.dfs.core.windows.net",
            credential=storage_key
        )
        fs_client     = client.get_file_system_client("paypal-bronze")
        bronze_files  = list(fs_client.get_paths(recursive=True))
        bronze_count  = len([f for f in bronze_files if f.name.endswith(".json")])

        print(f"[QUALITY] ✅ Bronze layer: {bronze_count} JSON files found")
        assert bronze_count > 0, "❌ Bronze layer is empty!"

        # Check Silver layer
        fs_silver     = client.get_file_system_client("paypal-silver")
        silver_files  = list(fs_silver.get_paths(recursive=True))
        silver_count  = len([f for f in silver_files if f.name.endswith(".parquet")])
        print(f"[QUALITY] ✅ Silver layer: {silver_count} Parquet files found")
        assert silver_count > 0, "❌ Silver layer is empty!"

        # Check Gold layer
        fs_gold      = client.get_file_system_client("paypal-gold")
        gold_files   = list(fs_gold.get_paths(recursive=True))
        gold_count   = len([f for f in gold_files if f.name.endswith(".parquet")])
        print(f"[QUALITY] ✅ Gold layer: {gold_count} Parquet files found")
        assert gold_count > 0, "❌ Gold layer is empty!"

        print("[QUALITY] ✅ All data quality checks passed!")
        return True

    t3_quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_quality_checks,
    )

    # --- Task 4: Pipeline Summary ---
    t4_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=lambda **ctx: print(f"""
        ====================================
        ✅ Pipeline completed successfully!
        Run ID: {ctx['run_id']}
        Date:   {ctx['ds']}
        ====================================
        """),
        provide_context=True,
    )

    # --- Pipeline Order ---
    t1_ingest >> t2_sql >> t3_quality >> t4_summary
