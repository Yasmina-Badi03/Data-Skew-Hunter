"""
metrics/cov.py
==============
Computes the Coefficient of Variation (CoV).

CoV measures the relative dispersion of partition sizes.
CoV = standard deviation / mean

Interpretation:
  CoV < 0.5  -> balanced distribution
  0.5 ≤ CoV < 1 -> slightly skewed
  CoV ≥ 1    -> skew detected (serious issue)
  CoV ≥ 2    -> severe skew
"""

import math
from typing import List


def compute_cov(partition_sizes: List[int]) -> float:
    """
    Computes the Coefficient of Variation (CoV) of partition sizes.

    Args:
        partition_sizes: list of integers representing the row count
                         in each Spark partition.

    Returns:
        float: CoV value (0 = perfectly balanced)
    """
    if not partition_sizes or len(partition_sizes) < 2:
        return 0.0

    n = len(partition_sizes)
    mean = sum(partition_sizes) / n

    if mean == 0:
        return 0.0

    # Variance = sum of squared deviations / n
    variance = sum((x - mean) ** 2 for x in partition_sizes) / n

    # Standard deviation
    std_dev = math.sqrt(variance)

    # CoV = std_dev / mean
    cov = std_dev / mean

    return round(cov, 4)


def cov_label(cov: float) -> str:
    """
    Returns a human-readable label based on the CoV value.

    Args:
        cov: Coefficient of Variation value

    Returns:
        str: severity label
    """
    if cov < 0.5:
        return "Balanced"
    elif cov < 1.0:
        return "Slightly skewed"
    elif cov < 2.0:
        return "Skew detected"
    else:
        return "Severe skew"