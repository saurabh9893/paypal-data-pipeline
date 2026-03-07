# tests/data_quality.py
# Data quality checks for all pipeline layers
# Run: python -m tests.data_quality

import os
import json
import glob
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY     = os.getenv("AZURE_STORAGE_KEY")

# Track results
results = []


def check(name: str, passed: bool, details: str = ""):
    """Records a quality check result."""
    status = "✅ PASS" if passed else "❌ FAIL"
    results.append({"check": name, "status": status, "details": details})
    print(f"  {status} | {name} {f'| {details}' if details else ''}")


def get_adls_client():
    account_url = f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=AZURE_STORAGE_KEY)


# ============================================================
# LAYER 1: Bronze Checks
# ============================================================
def check_bronze_layer():
    print("\n📦 BRONZE LAYER CHECKS")
    print("-" * 50)

    client    = get_adls_client()
    fs_client = client.get_file_system_client("paypal-bronze")
    paths     = list(fs_client.get_paths(recursive=True))
    json_files = [p for p in paths if p.name.endswith(".json")]

    # Check 1 - Files exist
    check("Bronze files exist", len(json_files) > 0,
          f"{len(json_files)} files found")

    # Check 2 - Files not empty
    empty_files = [p for p in json_files if p.content_length == 0]
    check("No empty Bronze files", len(empty_files) == 0,
          f"{len(empty_files)} empty files found")

    # Check 3 - Read and validate sample
    if json_files:
        sample_file  = json_files[0]
        file_client  = fs_client.get_file_client(sample_file.name)
        content      = file_client.download_file().readall()
        data         = json.loads(content)

        # Check 4 - Valid JSON structure
        check("Bronze JSON is valid", isinstance(data, list),
              f"Sample file has {len(data)} records")

        # Check 5 - Required fields exist
        if data:
            required_fields = ["id", "status", "purchase_units", "payer"]
            missing = [f for f in required_fields if f not in data[0]]
            check("Bronze required fields present", len(missing) == 0,
                  f"Missing: {missing}" if missing else "All fields present")

            # Check 6 - No null IDs
            null_ids = [r for r in data if not r.get("id")]
            check("No null order IDs in Bronze", len(null_ids) == 0,
                  f"{len(null_ids)} null IDs found")

            # Check 7 - Valid status values
            valid_statuses = {"CREATED", "COMPLETED", "CANCELLED", "REFUNDED"}
            invalid = [r for r in data
                      if r.get("status") not in valid_statuses]
            check("Valid status values in Bronze", len(invalid) == 0,
                  f"{len(invalid)} invalid statuses")


# ============================================================
# LAYER 2: Silver Checks
# ============================================================
def check_silver_layer():
    print("\n🥈 SILVER LAYER CHECKS")
    print("-" * 50)

    # Download sample silver file
    client    = get_adls_client()
    fs_client = client.get_file_system_client("paypal-silver")
    paths     = list(fs_client.get_paths(recursive=True))
    parquet_files = [p for p in paths if p.name.endswith(".parquet")]

    # Check 1 - Files exist
    check("Silver files exist", len(parquet_files) > 0,
          f"{len(parquet_files)} parquet files found")

    if parquet_files:
        # Download first parquet file
        sample = parquet_files[0]
        file_client = fs_client.get_file_client(sample.name)
        content     = file_client.download_file().readall()

        os.makedirs("data/quality_check", exist_ok=True)
        local_path = "data/quality_check/silver_sample.parquet"
        with open(local_path, "wb") as f:
            f.write(content)

        df = pd.read_parquet(local_path)

        # Check 2 - Row count
        check("Silver has rows", len(df) > 0,
              f"{len(df):,} rows in sample")

        # Check 3 - Required columns
        required_cols = [
            "order_id", "order_status", "product_name",
            "total_amount", "currency", "payer_email"
        ]
        missing_cols = [c for c in required_cols if c not in df.columns]
        check("Silver required columns present", len(missing_cols) == 0,
              f"Missing: {missing_cols}" if missing_cols else "All columns present")

        # Check 4 - No null order IDs
        null_ids = df["order_id"].isna().sum()
        check("No null order IDs in Silver", null_ids == 0,
              f"{null_ids} null IDs found")

        # Check 5 - No negative amounts
        if "total_amount" in df.columns:
            negative = (df["total_amount"] < 0).sum()
            check("No negative amounts in Silver", negative == 0,
                  f"{negative} negative amounts found")

        # Check 6 - No duplicates
        dupes = df["order_id"].duplicated().sum()
        check("No duplicate order IDs in Silver", dupes == 0,
              f"{dupes} duplicates found")

        # Check 7 - Valid currencies
        if "currency" in df.columns:
            valid_currencies = {"USD", "EUR", "GBP", "CAD", "AUD"}
            invalid_curr     = (~df["currency"].isin(valid_currencies)).sum()
            check("Valid currency codes in Silver", invalid_curr == 0,
                  f"{invalid_curr} invalid currencies")


# ============================================================
# LAYER 3: Gold Checks
# ============================================================
def check_gold_layer():
    print("\n🥇 GOLD LAYER CHECKS")
    print("-" * 50)

    client    = get_adls_client()
    fs_client = client.get_file_system_client("paypal-gold")
    paths     = list(fs_client.get_paths(recursive=True))
    parquet_files = [p for p in paths if p.name.endswith(".parquet")]

    # Check 1 - Files exist
    check("Gold files exist", len(parquet_files) > 0,
          f"{len(parquet_files)} parquet files found")

    # Check 2 - All 4 tables exist
    expected_tables = [
        "daily_revenue", "category_summary",
        "payment_summary", "country_summary"
    ]
    for table in expected_tables:
        table_files = [p for p in parquet_files if table in p.name]
        check(f"Gold table '{table}' exists", len(table_files) > 0)

    # Check 3 - Download and validate daily_revenue
    daily_files = [p for p in parquet_files if "daily_revenue" in p.name]
    if daily_files:
        file_client = fs_client.get_file_client(daily_files[0].name)
        content     = file_client.download_file().readall()
        local_path  = "data/quality_check/daily_revenue.parquet"

        with open(local_path, "wb") as f:
            f.write(content)

        df = pd.read_parquet(local_path)

        # Check 4 - Revenue is positive
        if "net_revenue" in df.columns:
            negative_rev = (df["net_revenue"] < 0).sum()
            check("No negative revenue in Gold", negative_rev == 0,
                  f"{negative_rev} negative revenue rows")

        # Check 5 - Total orders > 0
        if "total_orders" in df.columns:
            zero_orders = (df["total_orders"] <= 0).sum()
            check("All days have orders > 0", zero_orders == 0,
                  f"{zero_orders} days with zero orders")


# ============================================================
# SUMMARY
# ============================================================
def print_summary():
    print("\n" + "=" * 50)
    print("  DATA QUALITY SUMMARY")
    print("=" * 50)

    passed = [r for r in results if "PASS" in r["status"]]
    failed = [r for r in results if "FAIL" in r["status"]]

    print(f"  Total checks:  {len(results)}")
    print(f"  ✅ Passed:     {len(passed)}")
    print(f"  ❌ Failed:     {len(failed)}")

    if failed:
        print("\n  ❌ Failed checks:")
        for f in failed:
            print(f"     - {f['check']}: {f['details']}")
    else:
        print("\n  🎉 All checks passed!")

    print("=" * 50)
    return len(failed) == 0


if __name__ == "__main__":
    print("🔍 Starting Data Quality Checks...")
    print(f"   Timestamp: {datetime.now(timezone.utc).isoformat()}")

    os.makedirs("data/quality_check", exist_ok=True)

    check_bronze_layer()
    check_silver_layer()
    check_gold_layer()

    all_passed = print_summary()
    exit(0 if all_passed else 1)
