# transformations/silver_to_gold.py
# PySpark: Aggregations (Silver) → Business metrics (Gold)
# TODO: Will be implemented on Day 8

from pyspark.sql import SparkSession

def transform(silver_path: str, gold_path: str):
    """
    Reads Silver Parquet, computes KPIs, writes Gold layer.
    KPIs: revenue by day, refund rate, top payment methods, etc.
    """
    spark = SparkSession.builder.appName("PayPal-Silver-Gold").getOrCreate()
    # TODO Day 8: implement aggregations
    print(f"[SPARK] silver_to_gold: {silver_path} → {gold_path}")
    spark.stop()
