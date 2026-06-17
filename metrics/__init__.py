"""
metrics/
========
Spark distribution metrics module.
Contains: CoV (Coefficient of Variation) and Gini.
"""
from metrics.cov import compute_cov, cov_label
from metrics.gini import compute_gini, gini_label

__all__ = ["compute_cov", "cov_label", "compute_gini", "gini_label"]