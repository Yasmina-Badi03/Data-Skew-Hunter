"""
spark_jobs/generate_data.py
============================
Generates synthetic datasets with controlled skew for testing.

Available scenarios:
  1. heavy_skew   : one key represents 70% of the data
  2. medium_skew  : a few keys represent 30-50% of the data
  3. no_skew      : uniform distribution (baseline)
  4. realistic    : simulates realistic e-commerce transactions

Usage:
    spark-submit spark_jobs/generate_data.py
"""

import random
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)


# ─── Configuration ────────────────────────────────────────────────────────────

HDFS_BASE   = "hdfs://namenode:9000/data"
NUM_ROWS    = 500_000  # Number of rows per dataset


def create_spark():
    return (
        SparkSession.builder
        .appName("DataSkewHunter-DataGenerator")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )


# ─── Data generators ──────────────────────────────────────────────────────────

def generate_heavy_skew(spark: SparkSession, n: int = NUM_ROWS):
    """
    Dataset with very strong skew.
    One key ('USER_001') represents ~70% of the rows.

    Simulates a real case: a bot or power user generating
    an abnormally large number of transactions.
    """
    print(f"[GEN] Generating heavy_skew ({n} rows)...")

    # Key distribution: USER_001 = 70%, the rest = 30% spread over 99 keys
    def weighted_key():
        r = random.random()
        if r < 0.70:
            return "USER_001"      # hot key
        else:
            idx = random.randint(2, 100)
            return f"USER_{idx:03d}"

    data = [
        (
            weighted_key(),
            random.choice(["PROD_A", "PROD_B", "PROD_C", "PROD_D", "PROD_E"]),
            round(random.uniform(10.0, 500.0), 2),
            random.choice(["Paris", "Lyon", "Marseille", "Bordeaux", "Lille"])
        )
        for _ in range(n)
    ]

    schema = StructType([
        StructField("user_id",  StringType(),  True),
        StructField("product",  StringType(),  True),
        StructField("amount",   DoubleType(),  True),
        StructField("city",     StringType(),  True),
    ])

    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(f"{HDFS_BASE}/heavy_skew")
    df.write.mode("overwrite").option("header", True).csv(f"{HDFS_BASE}/heavy_skew_csv")
    print(f"[GEN] heavy_skew saved to {HDFS_BASE}/heavy_skew")
    return df


def generate_medium_skew(spark: SparkSession, n: int = NUM_ROWS):
    """
    Dataset with moderate skew.
    3 keys represent ~15% each (~45% total).
    """
    print(f"[GEN] Generating medium_skew ({n} rows)...")

    def medium_key():
        r = random.random()
        if r < 0.15:
            return "REGION_PARIS"
        elif r < 0.30:
            return "REGION_IDF"
        elif r < 0.45:
            return "REGION_SUD"
        else:
            idx = random.randint(1, 50)
            return f"REGION_{idx:02d}"

    data = [
        (
            medium_key(),
            random.choice(["CAT_A", "CAT_B", "CAT_C"]),
            random.randint(1, 1000),
            round(random.uniform(5.0, 200.0), 2)
        )
        for _ in range(n)
    ]

    schema = StructType([
        StructField("region",   StringType(),  True),
        StructField("category", StringType(),  True),
        StructField("quantity", IntegerType(), True),
        StructField("price",    DoubleType(),  True),
    ])

    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(f"{HDFS_BASE}/medium_skew")
    print(f"[GEN] medium_skew saved to {HDFS_BASE}/medium_skew")
    return df


def generate_no_skew(spark: SparkSession, n: int = NUM_ROWS):
    """
    Dataset without skew (uniform distribution).
    Serves as a reference to compare metrics.
    """
    print(f"[GEN] Generating no_skew ({n} rows)...")

    data = [
        (
            f"KEY_{random.randint(1, 1000):04d}",
            round(random.uniform(1.0, 100.0), 2),
            random.randint(1, 50)
        )
        for _ in range(n)
    ]

    schema = StructType([
        StructField("key",      StringType(),  True),
        StructField("value",    DoubleType(),  True),
        StructField("quantity", IntegerType(), True),
    ])

    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(f"{HDFS_BASE}/no_skew")
    print(f"[GEN] no_skew saved to {HDFS_BASE}/no_skew")
    return df


def generate_realistic_ecommerce(spark: SparkSession, n: int = NUM_ROWS):
    """
    Realistic e-commerce dataset.
    Simulates transactions with highly demanded popular products.

    Natural skew: a few best-selling products account for the majority
    of orders (Pareto law: 20% of products = 80% of sales).
    """
    print(f"[GEN] Generating realistic e-commerce ({n} rows)...")

    bestsellers = ["IPHONE15", "AIRPODS_PRO", "MACBOOK_AIR"]  # 60% of sales
    popular     = [f"PROD_{i:03d}" for i in range(1, 21)]      # 25%
    niche       = [f"NICHE_{i:03d}" for i in range(1, 201)]    # 15%

    def product_key():
        r = random.random()
        if r < 0.60:
            return random.choice(bestsellers)
        elif r < 0.85:
            return random.choice(popular)
        else:
            return random.choice(niche)

    countries = ["FR", "DE", "ES", "IT", "UK", "US", "CA", "JP", "AU", "BR"]
    statuses  = ["completed", "pending", "cancelled", "refunded"]

    data = [
        (
            f"CUST_{random.randint(1, 50000):05d}",
            product_key(),
            random.choice(countries),
            round(random.uniform(9.99, 1999.99), 2),
            random.choice(statuses),
            random.randint(1, 5)
        )
        for _ in range(n)
    ]

    schema = StructType([
        StructField("customer_id", StringType(),  True),
        StructField("product_id",  StringType(),  True),
        StructField("country",     StringType(),  True),
        StructField("amount",      DoubleType(),  True),
        StructField("status",      StringType(),  True),
        StructField("quantity",    IntegerType(), True),
    ])

    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(f"{HDFS_BASE}/ecommerce")
    df.write.mode("overwrite").option("header", True).csv(f"{HDFS_BASE}/ecommerce_csv")
    df.write.mode("overwrite").json(f"{HDFS_BASE}/ecommerce_json")
    print(f"[GEN] ecommerce saved to {HDFS_BASE}/ecommerce (parquet, csv, json)")
    return df


# ─── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    generate_heavy_skew(spark)
    generate_medium_skew(spark)
    generate_no_skew(spark)
    generate_realistic_ecommerce(spark)

    print("\n[DONE] All datasets generated successfully.")
    print(f"  Available datasets in: {HDFS_BASE}/")
    print("  - heavy_skew/    -> heavy skew (USER_001 = 70%)")
    print("  - medium_skew/   -> moderate skew (3 regions = 45%)")
    print("  - no_skew/       -> uniform distribution")
    print("  - ecommerce/     -> realistic e-commerce (Pareto 80/20)")

    spark.stop()