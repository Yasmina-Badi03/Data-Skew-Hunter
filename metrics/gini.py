"""
metrics/gini.py
===============
Computes the Gini coefficient.

Gini measures distribution inequality (borrowed from economics).
Gini = 0  -> perfectly equal distribution (each partition has the same row count)
Gini = 1  -> total inequality (one partition has everything)

In our Big Data context:
  Gini < 0.3  -> acceptable
  0.3 ≤ Gini < 0.6 -> warning
  Gini ≥ 0.6  -> significant skew
"""

from typing import List


def compute_gini(partition_sizes: List[int]) -> float:
    """
    Computes the Gini coefficient of partition sizes.

    Formula: Gini = (2 * Σ(i * x_i) / (n * Σ(x_i))) - (n+1)/n
    where x_i are the sizes sorted in ascending order.

    Args:
        partition_sizes: list of integers representing the row count
                         in each Spark partition.

    Returns:
        float: Gini value between 0 and 1.
    """
    if not partition_sizes or len(partition_sizes) < 2:
        return 0.0

    # Sort in ascending order for the standard Gini formula
    sorted_sizes = sorted(partition_sizes)
    n = len(sorted_sizes)
    total = sum(sorted_sizes)

    if total == 0:
        return 0.0

    # Gini formula
    # Σ (2*i - n - 1) * x_i  /  (n * total)
    numerator = sum((2 * (i + 1) - n - 1) * x for i, x in enumerate(sorted_sizes))
    gini = numerator / (n * total)

    return round(max(0.0, min(1.0, gini)), 4)


def gini_label(gini: float) -> str:
    """
    Returns a human-readable label based on the Gini value.

    Args:
        gini: Gini coefficient value (between 0 and 1)

    Returns:
        str: severity label
    """
    if gini < 0.3:
        return "Fair distribution"
    elif gini < 0.6:
        return "Moderate inequality"
    else:
        return "High inequality (Skew)"