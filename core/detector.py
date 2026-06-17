"""
core/detector.py
================
Spark data skew detection engine.

This module computes distribution metrics (CoV, Gini)
and decides whether skew is present and at what severity.

It works on two levels:
  1. Spark partition distribution (row counts)
  2. Candidate key value distribution (hot key analysis)

Usage:
    from core.detector import detect_skew, detect_key_skew
    result = detect_skew(partition_sizes)
    key_result = detect_key_skew(df, "user_id")
"""

from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from metrics.cov import compute_cov, cov_label
from metrics.gini import compute_gini, gini_label


# ─── Detection thresholds ─────────────────────────────────────────────────────

THRESHOLDS = {
    "cov_warning":  0.5,   # Warning
    "cov_skew":     1.0,   # Skew confirmed
    "cov_severe":   2.0,   # Severe skew
    "gini_warning": 0.3,   # Warning
    "gini_skew":    0.6,   # Skew confirmed
}


# ─── Spark partition detection ───────────────────────────────────────────────

def detect_skew(partition_sizes: List[int]) -> Dict[str, Any]:
    """
    Detects skew based on Spark partition sizes.

    This is the direct measure: how many rows are in each partition.
    Partition skew means one worker processes much more data than others,
    creating a bottleneck.

    Args:
        partition_sizes: list of integers, each = row count in one partition

    Returns:
        dict containing:
          - cov         : Coefficient of Variation
          - gini        : Gini coefficient
          - cov_label   : textual label for CoV
          - gini_label  : textual label for Gini
          - skew        : bool — True if skew detected
          - severity    : 'none' | 'low' | 'medium' | 'high' | 'critical'
          - max_size    : largest partition size
          - min_size    : smallest partition size
          - mean_size   : average partition size
          - max_ratio   : ratio max_size / mean_size
          - num_partitions : number of partitions
          - recommendation : recommended correction method
    """
    if not partition_sizes:
        return _empty_result()

    # ── Raw metrics ──
    n           = len(partition_sizes)
    total       = sum(partition_sizes)
    max_size    = max(partition_sizes)
    min_size    = min(partition_sizes)
    mean_size   = total / n if n > 0 else 0
    max_ratio   = max_size / mean_size if mean_size > 0 else 0

    # ── Statistical metrics ──
    cov  = compute_cov(partition_sizes)
    gini = compute_gini(partition_sizes)

    # ── Severity decision ──
    severity = _compute_severity(cov, gini)
    skew     = severity != "none"

    # ── Recommendation ──
    recommendation = _recommend_from_severity(severity, max_ratio)

    return {
        "cov":             cov,
        "gini":            gini,
        "cov_label":       cov_label(cov),
        "gini_label":      gini_label(gini),
        "skew":            skew,
        "severity":        severity,
        "max_size":        max_size,
        "min_size":        min_size,
        "mean_size":       round(mean_size, 2),
        "max_ratio":       round(max_ratio, 2),
        "num_partitions":  n,
        "total_rows":      total,
        "recommendation":  recommendation,
    }


def _compute_severity(cov: float, gini: float) -> str:
    """
    Computes skew severity based on CoV and Gini.

    Args:
        cov:  Coefficient of Variation
        gini: Coefficient of Gini

    Returns:
        str: 'none' | 'low' | 'medium' | 'high' | 'critical'
    """
    if cov >= THRESHOLDS["cov_severe"] or gini >= 0.85:
        return "critical"
    elif cov >= THRESHOLDS["cov_skew"] or gini >= THRESHOLDS["gini_skew"]:
        return "high"
    elif cov >= THRESHOLDS["cov_warning"] or gini >= THRESHOLDS["gini_warning"]:
        return "medium"
    elif cov > 0.1 or gini > 0.1:
        return "low"
    else:
        return "none"


def _recommend_from_severity(severity: str, max_ratio: float) -> str:
    """
    Returns a correction recommendation based on severity.

    Args:
        severity:  skew severity ('none', 'low', 'medium', 'high', 'critical')
        max_ratio: max partition / average ratio

    Returns:
        str: recommended correction method
    """
    if severity == "none":
        return "none"
    elif severity == "low":
        return "repartition"
    elif severity == "medium":
        return "repartition"
    elif severity == "high":
        if max_ratio > 10:
            return "salting"
        return "salting"
    else:  # critical
        return "salting"


def _empty_result() -> Dict[str, Any]:
    return {
        "cov": 0.0, "gini": 0.0,
        "cov_label": "N/A", "gini_label": "N/A",
        "skew": False, "severity": "none",
        "max_size": 0, "min_size": 0, "mean_size": 0,
        "max_ratio": 0, "num_partitions": 0, "total_rows": 0,
        "recommendation": "none"
    }


# ─── Key-based detection (hot key analysis) ──────────────────────────────────

def detect_key_skew(
    df: DataFrame,
    key_column: str,
    hot_threshold_pct: float = 5.0
) -> Dict[str, Any]:
    """
    Detects hot keys in a specific column.

    A hot key is a value that represents more than threshold_pct% of rows.
    It causes skew in groupBy / join operations on that column.

    Args:
        df:                Spark DataFrame
        key_column:        column to analyze
        hot_threshold_pct: threshold (pct) above which a value is a hot key

    Returns:
        dict containing:
          - key_column    : analyzed column
          - total_rows    : total number of rows
          - hot_keys      : detected hot keys with stats
          - hot_key_count : number of hot keys
          - skew_detected : bool
          - max_frequency : frequency of the most common value (pct)
    """
    total = df.count()
    if total == 0:
        return {"key_column": key_column, "total_rows": 0,
                "hot_keys": [], "hot_key_count": 0,
                "skew_detected": False, "max_frequency": 0.0}

    # Key distribution (top 20)
    freq_df = (
        df.groupBy(key_column)
        .count()
        .withColumn("pct", F.round(F.col("count") / total * 100, 2))
        .orderBy(F.desc("count"))
        .limit(20)
    )

    rows = freq_df.collect()

    hot_keys = []
    max_freq = 0.0

    for row in rows:
        pct = float(row["pct"])
        if pct > max_freq:
            max_freq = pct
        if pct >= hot_threshold_pct:
            hot_keys.append({
                "value":      str(row[key_column]),
                "count":      int(row["count"]),
                "percentage": pct
            })

    return {
        "key_column":    key_column,
        "total_rows":    total,
        "hot_keys":      hot_keys,
        "hot_key_count": len(hot_keys),
        "skew_detected": len(hot_keys) > 0,
        "max_frequency": max_freq,
        "top_distribution": [
            {"value": str(r[key_column]), "count": int(r["count"]), "pct": float(r["pct"])}
            for r in rows
        ]
    }