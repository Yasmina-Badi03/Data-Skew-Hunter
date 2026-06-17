"""
core/recommender.py
===================
Recommendation engine for skew correction.

This module combines metrics (CoV, Gini), hot keys,
and dataset characteristics to choose the BEST
correction method for each situation.

Decision rules:
  1. If CoV > 2 and hot keys detected          -> salting
  2. If CoV > 1 and few hot keys              -> repartition + light salting
  3. If CoV between 0.5 and 1                 -> repartition
  4. If small joined table (<= 500MB estimated) -> broadcast join
  5. If Spark 3 available                     -> AQE as a complement
  6. If CoV < 0.5                             -> no action needed

Usage:
    from core.recommender import recommend
    rec = recommend(detection_result, key_skew_result, dataset_info)
"""

from typing import Dict, Any, List


# ─── Thresholds ───────────────────────────────────────────────────────────────────

COV_SEVERE    = 2.0
COV_HIGH      = 1.0
COV_MEDIUM    = 0.5
GINI_SEVERE   = 0.75
GINI_HIGH     = 0.6
HOT_KEY_LIMIT = 20  # if num hot keys > 20 -> consider global repartition


# ─── Main recommendation logic ──────────────────────────────────────────────────

def recommend(
    partition_result: Dict[str, Any],
    key_result: Dict[str, Any] = None,
    spark_version: str = "3.0",
    estimated_table_mb: float = None
) -> Dict[str, Any]:
    """
    Generates a complete and detailed recommendation.

    Args:
        partition_result:   result from detect_skew(partition_sizes)
        key_result:         result from detect_key_skew() (optional)
        spark_version:      Spark version (for AQE)
        estimated_table_mb: estimated size of the small table in MB (for broadcast)

    Returns:
        dict containing:
          - primary_method     : main recommended method
          - secondary_methods  : complementary methods
          - priority           : 'urgent' | 'high' | 'medium' | 'low' | 'none'
          - confidence         : recommendation confidence level (0-1)
          - explanation        : explanation in English
          - params             : recommended parameters
          - steps              : ordered application steps
    """
    cov      = partition_result.get("cov", 0)
    gini     = partition_result.get("gini", 0)
    severity = partition_result.get("severity", "none")
    skew     = partition_result.get("skew", False)

    hot_keys     = (key_result or {}).get("hot_keys", [])
    hot_key_vals = [hk["value"] for hk in hot_keys]
    max_freq     = (key_result or {}).get("max_frequency", 0)

    # ── Case 1: No skew ──
    if not skew:
        return _no_action(cov, gini)

    # ── Case 2: Severe skew + hot key variables -> salting priority ──
    # Only recommend salting if a key is truly disproportionate (key skew)
    # If the keys are many and balanced, it's a cardinality issue, not a salting issue.
    is_hot_key_skew = False
    if len(hot_keys) > 0:
        if len(hot_keys) == 1:
            is_hot_key_skew = True # Only one dominant key
        else:
            min_freq = min([hk["percentage"] for hk in hot_keys])
            # If the largest key is at least 3x larger than the average of the others
            is_hot_key_skew = max_freq > (min_freq * 3)

    if severity in ("critical", "high") and is_hot_key_skew:
        salt_factor = _recommend_salt_factor(max_freq, len(hot_keys))
        rec = _build_rec(
            primary="salting",
            secondary=_aqe_if_available(spark_version),
            priority="urgent" if severity == "critical" else "high",
            confidence=0.9,
            explanation=(
                f"Severe skew caused by dominant hot keys (max={max_freq:.1f}%). "
                f"Salting is required to break these monolithic blocks."
            ),
            params={
                "salt_factor": salt_factor,
                "hot_keys":    hot_key_vals[:20],
            },
            steps=[
                f"1. Apply salting to the keys: {', '.join(hot_key_vals[:5])}",
                f"2. Utiliser un facteur de {salt_factor}"
            ]
        )
        return rec

    # ── Case 3: Moderate skew -> repartition ──
    elif severity in ("medium", "low"):
        n_partitions = _recommend_partitions(partition_result)
        return _build_rec(
            primary="repartition",
            secondary=_aqe_if_available(spark_version),
            priority="medium" if severity == "medium" else "low",
            confidence=0.75,
            explanation=(
                f"Moderate imbalance detected (CoV={cov:.2f}, Gini={gini:.2f}). "
                f"Repartitioning to {n_partitions} partitions should be sufficient. "
                "No dominant hot key identified."
            ),
            params={"num_partitions": n_partitions},
            steps=[
                f"1. Apply df.repartition({n_partitions})",
                "2. Check the new distribution",
                "3. (Optional) Enable AQE if Spark 3+"
            ]
        )

    # ── Case 4: Broadcast join (if small table) ──
    elif estimated_table_mb and estimated_table_mb <= 500:
        return _build_rec(
            primary="broadcast",
            secondary=[],
            priority="high",
            confidence=0.85,
            explanation=(
                f"The dimension table is estimated at {estimated_table_mb:.0f} MB. "
                "A broadcast join eliminates the large table shuffle "
                "and is usually the most efficient solution."
            ),
            params={"broadcast_threshold": "500MB"},
            steps=[
                "1. Use F.broadcast(df_small) during the join",
                "2. Configure spark.sql.autoBroadcastJoinThreshold if needed"
            ]
        )

    # ── Fallback ──
    else:
        return _build_rec(
            primary="repartition",
            secondary=_aqe_if_available(spark_version),
            priority="medium",
            confidence=0.6,
            explanation=(
                f"Skew detected (CoV={cov:.2f}) without a clear hot key. "
                "Repartitioning is recommended by default."
            ),
            params={"num_partitions": _recommend_partitions(partition_result)},
            steps=["1. Apply repartition", "2. Check the metrics"]
        )


# ─── Private helpers ───────────────────────────────────────────────────────────

def _recommend_salt_factor(max_freq_pct: float, num_hot_keys: int) -> int:
    """
    Recommends a salt factor based on the maximum hot key frequency.

    The more frequent the hot key, the higher the salt factor should be.
    """
    if max_freq_pct > 50:
        return 20
    elif max_freq_pct > 25:
        return 10
    elif max_freq_pct > 10:
        return 5
    else:
        return 3


def _recommend_partitions(partition_result: Dict[str, Any]) -> int:
    """
    Recommends a number of partitions based on total row count.
    """
    total = partition_result.get("total_rows", 0)
    current = partition_result.get("num_partitions", 10)

    if total > 10_000_000:
        return max(200, current * 2)
    elif total > 1_000_000:
        return max(100, current * 2)
    else:
        return max(50, current * 2)


def _aqe_if_available(spark_version: str) -> List[str]:
    """Returns 'aqe' if Spark >= 3.0, otherwise returns an empty list."""
    try:
        major = int(spark_version.split(".")[0])
        if major >= 3:
            return ["aqe"]
    except Exception:
        pass
    return []


def _no_action(cov: float, gini: float) -> Dict[str, Any]:
    return {
        "primary_method":    "none",
        "secondary_methods": [],
        "priority":          "none",
        "confidence":        1.0,
        "explanation":       (
            f"No significant skew detected (CoV={cov:.2f}, Gini={gini:.2f}). "
            "Partition distribution is acceptable."
        ),
        "params": {},
        "steps":  ["No action required."]
    }


def _build_rec(
    primary: str,
    secondary: List[str],
    priority: str,
    confidence: float,
    explanation: str,
    params: Dict,
    steps: List[str]
) -> Dict[str, Any]:
    return {
        "primary_method":    primary,
        "secondary_methods": secondary,
        "priority":          priority,
        "confidence":        round(confidence, 2),
        "explanation":       explanation,
        "params":            params,
        "steps":             steps
    }