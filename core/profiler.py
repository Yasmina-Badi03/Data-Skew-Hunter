"""
core/profiler.py
================
Automatic profiling of Spark DataFrame columns.

Goal:
  Analyze all columns to identify:
  - candidate skew keys
  - hot keys (over-represented values)
  - column distribution statistics

The profiler is fully automatic: it makes no assumptions
about dataset structure. It analyzes and returns scores.

Usage:
    from core.profiler import profile_dataframe, find_hot_keys
    profile = profile_dataframe(df)
    hot_keys = find_hot_keys(df, "user_id", top_n=10, threshold_pct=5.0)
"""

from typing import Dict, List, Tuple, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType
)


# ─── Constantes ────────────────────────────────────────────────────────────────

# Maximum cardinality threshold to consider a column as a grouping key.
# A column with 10 million distinct values is unlikely to be a useful
# grouping key for skew analysis (more like a unique ID per row).
MAX_CARDINALITY_RATIO = 0.95  # if distinct/total > 95% → not a skew candidate

# Minimum threshold: if a value represents more than X% of the total → hot key
HOT_KEY_THRESHOLD_PCT = 5.0   # 5%


# ─── Profiling principal ────────────────────────────────────────────────────────

def profile_dataframe(df: DataFrame, sample_fraction: float = 1.0) -> Dict[str, Any]:
    """
    Complete profiling of a Spark DataFrame.

    Analyzes each column and returns statistics useful for
    detecting data skew.

    Args:
        df:              Spark DataFrame to analyze
        sample_fraction: fraction to sample (1.0 = full dataset,
                         0.1 = 10% for very large datasets)

    Returns:
        dict containing:
          - total_rows   : total number of rows
          - num_cols     : number of columns
          - columns      : dict {column_name: column_stats}
          - candidates   : list of skew candidate columns
    """
    if 0 < sample_fraction < 1.0:
        df = df.sample(fraction=sample_fraction, seed=42)

    total_rows = df.count()

    if total_rows == 0:
        return {
            "total_rows": 0,
            "num_cols": len(df.columns),
            "columns": {},
            "candidates": []
        }

    columns_stats = {}
    candidates = []  # columns likely skew candidates

    for col_name in df.columns:
        stats = _profile_column(df, col_name, total_rows)
        columns_stats[col_name] = stats

        # A column is a candidate if:
        # 1. It is not a unique ID (cardinality ratio is not too high)
        # 2. It is not purely numeric (numeric metrics rarely group by raw value
        #    in real-world Big Data pipelines)
        # 3. It has a high hot key ratio (one value dominates)
        is_candidate = (
            stats["cardinality_ratio"] < MAX_CARDINALITY_RATIO
            and stats["cardinality"] > 1
            and stats["hot_key_ratio"] > 0
        )
        if is_candidate:
            candidates.append({
                "column": col_name,
                "hot_key_ratio": stats["hot_key_ratio"],
                "top_value":     stats["top_value"],
                "cardinality":   stats["cardinality"]
            })

    # Sort candidates by hot_key_ratio descending
    candidates.sort(key=lambda x: x["hot_key_ratio"], reverse=True)

    return {
        "total_rows": total_rows,
        "num_cols":   len(df.columns),
        "columns":    columns_stats,
        "candidates": candidates
    }


def _profile_column(df: DataFrame, col_name: str, total_rows: int) -> Dict[str, Any]:
    """
    Profile an individual column.

    Args:
        df:         Spark DataFrame
        col_name:   name of the column to profile
        total_rows: total number of rows in the DataFrame

    Returns:
        dict with the column statistics
    """
    try:
        # Count distinct values
        cardinality = df.select(col_name).distinct().count()
        cardinality_ratio = cardinality / total_rows if total_rows > 0 else 0

        # Top value (most frequent)
        top_row = (
            df.groupBy(col_name)
            .count()
            .orderBy(F.desc("count"))
            .first()
        )

        if top_row:
            top_value = str(top_row[col_name])
            top_count = int(top_row["count"])
            hot_key_ratio = (top_count / total_rows) * 100
        else:
            top_value = None
            top_count = 0
            hot_key_ratio = 0.0

        return {
            "cardinality":       cardinality,
            "cardinality_ratio": round(cardinality_ratio, 4),
            "top_value":         top_value,
            "top_count":         top_count,
            "hot_key_ratio":     round(hot_key_ratio, 2),  # en %
        }

    except Exception as e:
        # If a column causes an issue (complex type, etc.)
        return {
            "cardinality":       -1,
            "cardinality_ratio": -1,
            "top_value":         None,
            "top_count":         0,
            "hot_key_ratio":     0.0,
            "error":             str(e)
        }


# ─── Hot key analysis ───────────────────────────────────────────────────────────

def find_hot_keys(
    df: DataFrame,
    key_column: str,
    top_n: int = 10,
    threshold_pct: float = HOT_KEY_THRESHOLD_PCT
) -> List[Dict[str, Any]]:
    """
    Finds hot keys in a given column.

    A "hot key" is a value whose frequency exceeds the threshold_pct.

    Args:
        df:            Spark DataFrame
        key_column:    column to analyze
        top_n:         number of top values to return
        threshold_pct: threshold (pct) above which a value is considered hot

    Returns:
        list of dicts {value, count, percentage, is_hot}
    """
    total = df.count()
    if total == 0:
        return []

    top_rows = (
        df.groupBy(key_column)
        .count()
        .orderBy(F.desc("count"))
        .limit(top_n)
        .collect()
    )

    result = []
    for row in top_rows:
        pct = (row["count"] / total) * 100
        result.append({
            "value":      str(row[key_column]),
            "count":      int(row["count"]),
            "percentage": round(pct, 2),
            "is_hot":     pct >= threshold_pct
        })

    return result


def get_key_distribution(
    df: DataFrame,
    key_column: str,
    limit: int = 50
) -> List[Tuple[str, int]]:
    """
    Returns the full distribution of a column (top N values).

    Args:
        df:         Spark DataFrame
        key_column: column to analyze
        limit:      maximum number of values to return

    Returns:
        list of tuples (value, count) sorted by descending frequency
    """
    rows = (
        df.groupBy(key_column)
        .count()
        .orderBy(F.desc("count"))
        .limit(limit)
        .collect()
    )
    return [(str(r[key_column]), int(r["count"])) for r in rows]


def auto_select_key_column(profile: Dict[str, Any]) -> str:
    """
    Automatically selects the best key column to analyze.

    Chooses the column with the highest hot_key_ratio among candidates.

    Args:
        profile: result of profile_dataframe()

    Returns:
        str: name of the recommended column, or None if none found
    """
    candidates = profile.get("candidates", [])
    if not candidates:
        return None
    return candidates[0]["column"]