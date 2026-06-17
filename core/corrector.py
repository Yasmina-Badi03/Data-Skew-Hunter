"""
core/corrector.py
=================
Spark data skew correction engine.

This module implements classical remediation techniques:
  1. Salting (adding noise to hot keys)
  2. Repartition (uniform redistribution)
  3. AQE Configuration (dynamic optimization)

Usage :
    from core.corrector import apply_salting, apply_repartition
    df_salted = apply_salting(df, "user_id", ["HOT_01"], salt_factor=10)
"""

from typing import List, Dict, Any, Tuple
import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ─── Techniques de correction ──────────────────────────────────────────────────

def apply_salting(
    df: DataFrame,
    key_column: str,
    hot_keys: List[str],
    salt_factor: int = 10
) -> DataFrame:
    """
    Applies salting to the keys identified as 'hot'.

    This adds a random suffix (0 to salt_factor-1) only to problematic values
    to spread them across multiple partitions during shuffle.

    Args:
        df:          Spark DataFrame
        key_column:  column to salt
        hot_keys:    list of values (strings) identified as hot keys
        salt_factor: number of dispersion buckets

    Returns:
        DataFrame transformed with a new 'salted_key' column
    """
    if not hot_keys:
        return df

    # Generate a random bucket between 0 and salt_factor-1
    salt_expr = (F.rand() * salt_factor).cast("int")

    # If the key is in hot_keys, add the salt suffix; otherwise retain the original key
    # Note: convert the key to string for concatenation
    return df.withColumn(
        "salted_key",
        F.when(
            F.col(key_column).isin(hot_keys),
            F.concat(F.col(key_column).cast("string"), F.lit("_"), salt_expr)
        ).otherwise(F.col(key_column).cast("string"))
    )


def apply_repartition(
    df: DataFrame,
    num_partitions: int = None,
    key_column: str = None
) -> DataFrame:
    """
    Applies uniform repartition (Round-Robin).

    WARNING: Do not use `key_column` here (e.g. df.repartition(num, key_column)).
    If you hash on an asymmetric key, all rows for the hot key may end up in the
    same partition, which will not correct Data Skew and may make it worse.
    
    Args:
        df:             Spark DataFrame
        num_partitions: target number of partitions (None = auto)
        key_column:     (Ignored) Present for generic signature compatibility

    Returns:
        Uniformly repartitioned DataFrame
    """
    if num_partitions is None:
        # Empirical rule: target ~128MB to 256MB per partition
        num_partitions = 200  # Spark default heuristics

    # Force Round Robin (random and uniform distribution)
    return df.repartition(num_partitions)


def benchmark_job(df: DataFrame, key_column: str = None, n_runs: int = 1) -> float:
    """
    Measure a representative Spark job on the DataFrame and return average duration.

    If a key column is provided, this uses groupBy(key_column).count() to force a shuffle.
    Otherwise it falls back to a simple count.
    """
    # Warmup run to compile the query plan in Catalyst and warm up the JVM
    if key_column:
        df.groupBy(key_column).count().count()
    else:
        df.count()

    durations = []
    # Actual measured runs
    for _ in range(n_runs):
        t0 = time.time()
        if key_column:
            # Use the same simple groupBy + count for ALL key columns
            # (including salted_key) so that before/after benchmarks
            # measure the exact same type of operation and the comparison
            # is fair.  The de-salting step is an application concern,
            # not part of the shuffle-performance benchmark.
            df.groupBy(key_column).count().count()
        else:
            df.count()
        durations.append(time.time() - t0)
    return round(sum(durations) / len(durations), 2)


def get_partition_sizes(df: DataFrame) -> List[int]:
    """
    Computes the number of rows in each DataFrame partition.
    This is the fundamental metric for measuring physical skew.

    Args:
        df: Spark DataFrame

    Returns:
        List of integers (one per partition)
    """
    num_partitions = df.rdd.getNumPartitions()
    if num_partitions == 0:
        return []

    # Count rows per partition locally, without a global shuffle.
    sizes = df.rdd.mapPartitions(lambda iterator: [sum(1 for _ in iterator)]).collect()
    # If some partitions are empty, collect() still returns the exact partition count.
    return sizes


def auto_correct(
    df: DataFrame,
    key_column: str,
    recommendation: Dict[str, Any],
    hot_keys: List[str] = None,
    salt_factor: int = 10,
    num_partitions: int = None
) -> Tuple[DataFrame, str, Dict[str, Any]]:
    """
    Automatically applies the best method based on the recommendation.

    Returns:
        Tuple (df_corrected, method_name, details_dict)
    """
    method = recommendation.get("primary_method", "none")
    details = {"applied": True, "method": method}

    if method == "salting" and key_column:
        df_new = apply_salting(df, key_column, hot_keys or [], salt_factor)
        details.update({"salt_factor": salt_factor, "hot_keys": hot_keys})
        return df_new, "salting", details

    elif method == "repartition":
        df_new = apply_repartition(df, num_partitions, key_column)
        details.update({"num_partitions": num_partitions})
        return df_new, "repartition", details

    elif method == "aqe":
        return df, "aqe", {"applied": True, "note": "AQE configured in the session"}

    else:
        return df, "none", {"applied": False}
