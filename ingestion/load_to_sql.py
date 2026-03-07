# ingestion/load_to_sql.py
# Loads Gold Parquet files from Azure → Azure SQL Database
# Run: python -m ingestion.load_to_sql

import os
import pyodbc
import pandas as pd
import glob
import shutil
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

# --- Azure Config ---
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY     = os.getenv("AZURE_STORAGE_KEY")
GOLD_CONTAINER        = "paypal-gold"
LOCAL_GOLD_PATH       = "data/gold_download/"

# --- Azure SQL Config ---
SQL_SERVER   = os.getenv("AZURE_SQL_SERVER")
SQL_DB       = os.getenv("AZURE_SQL_DB")
SQL_USER     = os.getenv("AZURE_SQL_USER")
SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")


def get_adls_client():
    account_url = f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=AZURE_STORAGE_KEY)


def get_sql_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DB};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def download_gold_files():
    """Downloads Gold Parquet files from Azure."""
    print("[DOWNLOAD] Connecting to Azure Gold layer...")
    client    = get_adls_client()
    fs_client = client.get_file_system_client(GOLD_CONTAINER)

    if os.path.exists(LOCAL_GOLD_PATH):
        shutil.rmtree(LOCAL_GOLD_PATH)
    os.makedirs(LOCAL_GOLD_PATH, exist_ok=True)

    paths         = list(fs_client.get_paths(recursive=True))
    parquet_files = [p for p in paths if p.name.endswith(".parquet")]
    print(f"[DOWNLOAD] Found {len(parquet_files)} parquet files")

    for path in parquet_files:
        file_client = fs_client.get_file_client(path.name)
        content     = file_client.download_file().readall()

        # Preserve folder structure
        local_dir  = os.path.join(LOCAL_GOLD_PATH, os.path.dirname(path.name))
        os.makedirs(local_dir, exist_ok=True)
        local_file = os.path.join(LOCAL_GOLD_PATH, path.name)

        with open(local_file, "wb") as f:
            f.write(content)
        print(f"[DOWNLOAD] ✅ {path.name}")

    print(f"[DOWNLOAD] Done!\n")

    # Debug: show what was downloaded
    print("[DEBUG] Downloaded files:")
    for root, dirs, files in os.walk(LOCAL_GOLD_PATH):
        for file in files:
            full_path = os.path.join(root, file)
            print(f"  {full_path}")


def create_tables(conn):
    print("[SQL] Creating tables...")
    cursor = conn.cursor()

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='daily_revenue')
        CREATE TABLE daily_revenue (
            order_date       DATE,
            total_orders     INT,
            total_revenue    FLOAT,
            net_revenue      FLOAT,
            avg_order_value  FLOAT,
            total_refunds    INT
        )
    """)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='category_summary')
        CREATE TABLE category_summary (
            product_category  VARCHAR(100),
            total_orders      INT,
            total_revenue     FLOAT,
            avg_order_value   FLOAT,
            refund_rate_pct   FLOAT
        )
    """)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='payment_summary')
        CREATE TABLE payment_summary (
            payment_method   VARCHAR(100),
            total_orders     INT,
            total_revenue    FLOAT,
            avg_order_value  FLOAT
        )
    """)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='country_summary')
        CREATE TABLE country_summary (
            country_code     VARCHAR(10),
            total_orders     INT,
            total_revenue    FLOAT,
            avg_order_value  FLOAT
        )
    """)
    conn.commit()
    print("[SQL] ✅ Tables ready!\n")


def load_table(conn, table_name: str):
    """Reads all parquet files for a table and loads into SQL."""

    # Search recursively for parquet files
    pattern       = os.path.join(LOCAL_GOLD_PATH, "**", "*.parquet")
    all_files     = glob.glob(pattern, recursive=True)

    # Filter files belonging to this table
    table_files = [f for f in all_files if table_name in f.replace("\\", "/")]

    print(f"[SQL] Loading {table_name}: found {len(table_files)} files...")

    if not table_files:
        print(f"[SQL] ⚠️  No files found for {table_name}\n")
        return

    # Read all into one DataFrame
    df = pd.concat([pd.read_parquet(f) for f in table_files])
    print(f"[SQL] {len(df):,} rows to insert...")

    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table_name}")

    cols         = ", ".join(df.columns)
    placeholders = ", ".join(["?" for _ in df.columns])

    for _, row in df.iterrows():
        cursor.execute(
            f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})",
            tuple(row)
        )

    conn.commit()
    print(f"[SQL] ✅ {table_name}: {len(df):,} rows loaded!\n")


if __name__ == "__main__":
    # Step 1 - Download Gold from Azure
    download_gold_files()

    # Step 2 - Connect to Azure SQL
    print("[SQL] Connecting to Azure SQL...")
    conn = get_sql_connection()
    print("[SQL] ✅ Connected!\n")

    # Step 3 - Create tables
    create_tables(conn)

    # Step 4 - Load each Gold table
    load_table(conn, "daily_revenue")
    load_table(conn, "category_summary")
    load_table(conn, "payment_summary")
    load_table(conn, "country_summary")

    conn.close()

    print("🎉 All Gold tables loaded into Azure SQL!")
    print(f"   Server:   {SQL_SERVER}")
    print(f"   Database: {SQL_DB}")
