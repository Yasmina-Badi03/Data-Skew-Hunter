"""
core/loader.py
==============
Universal dataset loader for Spark.

Supports:
  - CSV  (local or HDFS)
  - JSON (local or HDFS)
  - Parquet (local or HDFS)
  - ORC
  - Auto-detection of format by extension

Usage :
    spark = SparkSession.builder...getOrCreate()
    df = load_dataset(spark, "hdfs://namenode:9000/data/file.parquet")
    df = load_dataset(spark, "/local/path/data.csv")
    df = load_dataset(spark, "hdfs://namenode:9000/data/dir", fmt="parquet")
"""

import os
from pyspark.sql import SparkSession, DataFrame


def detect_format(path: str) -> str:
    """
    Automatically detects the format from the file extension.

    Args:
        path: file or directory path

    Returns:
        str: detected format ('csv', 'json', 'parquet', 'orc')

    Raises:
        ValueError: if the format is not recognized
    """
    path_lower = path.lower().rstrip("/")

    if path_lower.endswith(".csv"):
        return "csv"
    elif path_lower.endswith(".json"):
        return "json"
    elif path_lower.endswith(".parquet"):
        return "parquet"
    elif path_lower.endswith(".orc"):
        return "orc"
    else:
        # Default: parquet (native Spark format)
        return "parquet"


def load_dataset(
    spark: SparkSession,
    path: str,
    fmt: str = None,
    header: bool = True,
    infer_schema: bool = True
) -> DataFrame:
    """
    Loads a dataset of any supported format into a Spark DataFrame.

    Args:
        spark:        active SparkSession
        path:         file path (local or HDFS, e.g. hdfs://namenode:9000/...)
        fmt:          explicit format ('csv','json','parquet','orc')
                      If None, auto-detected from the extension.
        header:       True for CSV header row (ignored for other formats)
        infer_schema: True to infer types automatically

    Returns:
        Loaded Spark DataFrame ready for use

    Examples:
        df = load_dataset(spark, "data/transactions.csv")
        df = load_dataset(spark, "hdfs://namenode:9000/data/logs", fmt="parquet")
    """
    if fmt is None:
        fmt = detect_format(path)

    fmt = fmt.lower()

    if fmt == "csv":
        df = (
            spark.read
            .option("header", str(header).lower())
            .option("inferSchema", str(infer_schema).lower())
            .option("multiLine", "true")    # support multi-line fields
            .option("escape", '"')          # support quoted values
            .csv(path)
        )

    elif fmt == "json":
        df = (
            spark.read
            .option("multiLine", "true")    # support multi-line JSON
            .json(path)
        )

    elif fmt == "parquet":
        df = spark.read.parquet(path)

    elif fmt == "orc":
        df = spark.read.orc(path)

    else:
        raise ValueError(
            f"Unsupported format: '{fmt}'. "
            "Valid formats: csv, json, parquet, orc"
        )

    return df


def get_dataset_info(df: DataFrame) -> dict:
    """
    Returns basic information about a loaded DataFrame.

    Args:
        df: Spark DataFrame

    Returns:
        dict with dataset information:
          - columns   : list of columns
          - num_cols  : number of columns
          - row_count : number of rows (Spark action triggers computation)
          - schema    : dict {column: type}
    """
    return {
        "columns":   df.columns,
        "num_cols":  len(df.columns),
        "row_count": df.count(),
        "schema":    {f.name: str(f.dataType) for f in df.schema.fields}
    }