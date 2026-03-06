# transformations/bronze_to_silver.py
# PySpark: Clean raw JSON (Bronze) → structured Parquet (Silver)
# TODO: Will be implemented on Day 6

from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "PayPal-Bronze-Silver") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def transform(bronze_path: str, silver_path: str):
    """
    Reads raw JSON from Bronze, cleans & flattens, writes Parquet to Silver.
    """
    spark = create_spark_session()
    # TODO Day 6: implement transformations
    print(f"[SPARK] bronze_to_silver: {bronze_path} → {silver_path}")
    spark.stop()


if __name__ == "__main__":
    transform("data/bronze/", "data/silver/")
