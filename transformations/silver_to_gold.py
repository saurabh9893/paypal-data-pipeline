# transformations/silver_to_gold.py
# PySpark: Aggregations (Silver) -> Business metrics (Gold)
# Flow: Download Silver -> Aggregate -> Upload Gold to Azure
# Run: python -m transformations.silver_to_gold

import os
import glob
import shutil
os.environ["JAVA_HOME"]   = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"]        = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    round as spark_round, to_date,
    max as spark_max, min as spark_min
)
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

# --- Azure Config ---
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY     = os.getenv("AZURE_STORAGE_KEY")
SILVER_CONTAINER      = "paypal-silver"
GOLD_CONTAINER        = "paypal-gold"
LOCAL_SILVER_PATH     = "data/silver_download/"
LOCAL_GOLD_PATH       = "data/gold/"


def get_adls_client():
    account_url = f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=AZURE_STORAGE_KEY)


def download_silver_files():
    """Downloads Parquet files from Azure Silver layer."""
    print("[DOWNLOAD] Connecting to Azure Silver layer...")
    client    = get_adls_client()
    fs_client = client.get_file_system_client(SILVER_CONTAINER)

    if os.path.exists(LOCAL_SILVER_PATH):
        shutil.rmtree(LOCAL_SILVER_PATH)
    os.makedirs(LOCAL_SILVER_PATH, exist_ok=True)

    paths         = list(fs_client.get_paths(recursive=True))
    parquet_files = [p for p in paths if p.name.endswith(".parquet")]
    print(f"[DOWNLOAD] Found {len(parquet_files)} parquet files")

    for path in parquet_files:
        file_client = fs_client.get_file_client(path.name)
        content     = file_client.download_file().readall()

        local_file = os.path.join(LOCAL_SILVER_PATH, path.name.replace("/", "_"))
        with open(local_file, "wb") as f:
            f.write(content)

    print(f"[DOWNLOAD] Done! {len(parquet_files)} files downloaded.\n")


def create_spark_session():
    return SparkSession.builder \
        .appName("PayPal-Silver-Gold") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def compute_gold_tables(spark):
    """Reads Silver Parquet and computes all Gold aggregations."""
    print("[SPARK] Reading Silver data...")
    df = spark.read.parquet(LOCAL_SILVER_PATH)
    print(f"[SPARK] Silver records loaded: {df.count():,}")

    # Add date column for grouping
    df = df.withColumn("order_date", to_date(col("created_at")))

    # --- Gold Table 1: Daily Revenue ---
    print("\n[SPARK] Computing daily_revenue...")
    daily_revenue = df.groupBy("order_date").agg(
        count("order_id").alias("total_orders"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_round(spark_sum("revenue"), 2).alias("net_revenue"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        count(col("is_refunded") == True).alias("total_refunds")
    ).orderBy("order_date")
    daily_revenue.show(5)

    # --- Gold Table 2: Category Summary ---
    print("\n[SPARK] Computing category_summary...")
    category_summary = df.groupBy("product_category").agg(
        count("order_id").alias("total_orders"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        spark_round(
            (count(col("is_refunded") == True) / count("order_id") * 100), 2
        ).alias("refund_rate_pct")
    ).orderBy(col("total_revenue").desc())
    category_summary.show()

    # --- Gold Table 3: Payment Method Summary ---
    print("\n[SPARK] Computing payment_method_summary...")
    payment_summary = df.groupBy("payment_method").agg(
        count("order_id").alias("total_orders"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value")
    ).orderBy(col("total_orders").desc())
    payment_summary.show()

    # --- Gold Table 4: Country Summary ---
    print("\n[SPARK] Computing country_summary...")
    country_summary = df.groupBy("country_code").agg(
        count("order_id").alias("total_orders"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value")
    ).orderBy(col("total_revenue").desc())
    country_summary.show(10)


    return {
        "daily_revenue":     daily_revenue,
        "category_summary":  category_summary,
        "payment_summary":   payment_summary,
        "country_summary":   country_summary
    }


def write_gold_locally(gold_tables: dict):
    """Writes all Gold tables as Parquet locally."""
    if os.path.exists(LOCAL_GOLD_PATH):
        shutil.rmtree(LOCAL_GOLD_PATH)
    os.makedirs(LOCAL_GOLD_PATH, exist_ok=True)

    for table_name, df in gold_tables.items():
        path = os.path.join(LOCAL_GOLD_PATH, table_name)
        df.write.mode("overwrite").parquet(path)
        print(f"[SPARK] ✅ Written: {table_name}")


def upload_gold_to_azure():
    """Uploads all Gold Parquet files to Azure Gold container."""
    print("\n[UPLOAD] Uploading Gold data to Azure...")
    client    = get_adls_client()
    fs_client = client.get_file_system_client(GOLD_CONTAINER)

    parquet_files = glob.glob(f"{LOCAL_GOLD_PATH}**/*.parquet", recursive=True)
    print(f"[UPLOAD] Found {len(parquet_files)} parquet files to upload")

    for local_file in parquet_files:
        azure_path = local_file.replace(LOCAL_GOLD_PATH, "").replace("\\", "/")
        with open(local_file, "rb") as f:
            content = f.read()
        file_client = fs_client.get_file_client(azure_path)
        file_client.upload_data(content, overwrite=True)
        print(f"[UPLOAD] ✅ {azure_path}")

    print(f"[UPLOAD] Done! {len(parquet_files)} files uploaded to Azure Gold.")


if __name__ == "__main__":
    # Step 1 - Download Silver from Azure
    download_silver_files()

    # Step 2 - Compute Gold aggregations
    spark = create_spark_session()
    try:
        gold_tables = compute_gold_tables(spark)
        write_gold_locally(gold_tables)
    finally:
        spark.stop()
        print("[SPARK] Session stopped.")

    # Step 3 - Upload Gold to Azure
    upload_gold_to_azure()

    print("\n Congratulations Silver to Gold transformation complete!")
    print(f"   Silver: Azure/{SILVER_CONTAINER}")
    print(f"   Gold:   Azure/{GOLD_CONTAINER}")
    print("\n Gold Tables Created:")
    print("   - daily_revenue")
    print("   - category_summary")
    print("   - payment_method_summary")
    print("   - country_summary")
    print("   - order_status_summary")
