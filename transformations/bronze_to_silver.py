# transformations/bronze_to_silver.py
# PySpark: Clean raw JSON (Bronze) → structured Parquet (Silver)
# Flow: Download from Azure → Transform locally → Upload to Azure Silver
# Run: python -m transformations.bronze_to_silver

import os
import json
import glob
import shutil
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, concat_ws, to_timestamp,
    when, lit, round as spark_round
)
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

# --- Azure Config ---
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY     = os.getenv("AZURE_STORAGE_KEY")
BRONZE_CONTAINER      = "paypal-bronze"
SILVER_CONTAINER      = "paypal-silver"
LOCAL_BRONZE_PATH     = "data/bronze/"
LOCAL_SILVER_PATH     = "data/silver/"


def get_adls_client():
    """Returns Azure Data Lake client."""
    account_url = f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
    return DataLakeServiceClient(
        account_url=account_url,
        credential=AZURE_STORAGE_KEY
    )


def download_bronze_files():
    """Downloads all JSON files from Azure Bronze layer to local folder."""
    print("[DOWNLOAD] Connecting to Azure Data Lake...")
    client    = get_adls_client()
    fs_client = client.get_file_system_client(BRONZE_CONTAINER)

    # Clean local bronze folder
    if os.path.exists(LOCAL_BRONZE_PATH):
        shutil.rmtree(LOCAL_BRONZE_PATH)
    os.makedirs(LOCAL_BRONZE_PATH, exist_ok=True)

    # List all files in Bronze container
    paths = list(fs_client.get_paths(recursive=True))
    json_files = [p for p in paths if p.name.endswith(".json")]

    print(f"[DOWNLOAD] Found {len(json_files)} files in Bronze layer")

    for path in json_files:
        file_client = fs_client.get_file_client(path.name)
        download     = file_client.download_file()
        content      = download.readall()

        # Save locally with flat filename
        local_filename = path.name.replace("/", "_")
        local_path     = os.path.join(LOCAL_BRONZE_PATH, local_filename)

        with open(local_path, "wb") as f:
            f.write(content)

        print(f"[DOWNLOAD] ✅ {path.name} → {local_path}")

    print(f"[DOWNLOAD] Done! {len(json_files)} files downloaded.\n")
    return len(json_files)


def create_spark_session():
    """Creates a local Spark session."""
    return SparkSession.builder \
        .appName("PayPal-Bronze-Silver") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def transform(spark, bronze_path: str):
    """Flattens nested JSON into clean structured table."""
    print(f"[SPARK] Reading Bronze data from: {bronze_path}")
    df = spark.read.option("multiline", "true").json(bronze_path)
    print(f"[SPARK] Raw records loaded: {df.count():,}")

    print("[SPARK] Starting transformation...")

    # Explode purchase_units array
    df = df.withColumn("purchase_unit", explode(col("purchase_units")))

    # Flatten all nested fields
    df_silver = df.select(
        col("id").alias("order_id"),
        col("status").alias("order_status"),
        col("intent"),
        to_timestamp(col("create_time")).alias("created_at"),
        to_timestamp(col("update_time")).alias("updated_at"),
        col("purchase_unit.description").alias("product_name"),
        col("purchase_unit.category").alias("product_category"),
        col("purchase_unit.quantity").alias("quantity"),
        col("purchase_unit.payment_method").alias("payment_method"),
        col("purchase_unit.amount.currency_code").alias("currency"),
        col("purchase_unit.amount.value").cast(DoubleType()).alias("total_amount"),
        col("purchase_unit.amount.breakdown.item_total.value")
            .cast(DoubleType()).alias("unit_price"),
        concat_ws(" ",
            col("payer.name.given_name"),
            col("payer.name.surname")
        ).alias("payer_name"),
        col("payer.email_address").alias("payer_email"),
        col("payer.payer_id").alias("payer_id"),
        col("payer.address.country_code").alias("country_code"),
    )

    # Add derived columns
    df_silver = df_silver \
        .withColumn("is_completed",
            when(col("order_status") == "COMPLETED", True).otherwise(False)) \
        .withColumn("is_refunded",
            when(col("order_status") == "REFUNDED", True).otherwise(False)) \
        .withColumn("revenue",
            when(col("order_status") == "COMPLETED",
                spark_round(col("total_amount"), 2)
            ).otherwise(lit(0.0))
        )

    # Drop nulls on critical fields
    df_silver = df_silver.dropna(subset=["order_id", "total_amount"])

    print(f"[SPARK] Silver records: {df_silver.count():,}")
    print("[SPARK] Sample Silver data:")
    df_silver.show(5, truncate=True)

    return df_silver


def upload_silver_to_azure(silver_path: str):
    """Uploads Silver Parquet files to Azure Silver container."""
    print("\n[UPLOAD] Uploading Silver data to Azure...")
    client    = get_adls_client()
    fs_client = client.get_file_system_client(SILVER_CONTAINER)

    parquet_files = glob.glob(f"{silver_path}**/*.parquet", recursive=True)
    print(f"[UPLOAD] Found {len(parquet_files)} parquet files to upload")

    for local_file in parquet_files:
        # Create Azure path
        azure_path = local_file.replace(silver_path, "").replace("\\", "/")
        azure_path = f"silver/{azure_path}"

        with open(local_file, "rb") as f:
            content = f.read()

        file_client = fs_client.get_file_client(azure_path)
        file_client.upload_data(content, overwrite=True)
        print(f"[UPLOAD] ✅ Uploaded: {azure_path}")

    print(f"[UPLOAD] Done! {len(parquet_files)} files uploaded to Azure Silver.")


if __name__ == "__main__":
    os.makedirs(LOCAL_SILVER_PATH, exist_ok=True)

    # Step 1 - Download from Azure Bronze
    download_bronze_files()

    # Step 2 - Transform with PySpark
    spark = create_spark_session()
    try:
        df_silver = transform(spark, LOCAL_BRONZE_PATH)

        # Write locally as Parquet
        print(f"\n[SPARK] Writing Silver Parquet to: {LOCAL_SILVER_PATH}")
        df_silver.write \
            .mode("overwrite") \
            .partitionBy("order_status") \
            .parquet(LOCAL_SILVER_PATH)
        print("[SPARK] ✅ Silver layer written locally!")

    finally:
        spark.stop()
        print("[SPARK] Session stopped.")

    # Step 3 - Upload Silver to Azure
    upload_silver_to_azure(LOCAL_SILVER_PATH)

    print("\n🎉 Bronze → Silver transformation complete!")
    print(f"   Bronze: Azure/{BRONZE_CONTAINER}")
    print(f"   Silver: Azure/{SILVER_CONTAINER}")
