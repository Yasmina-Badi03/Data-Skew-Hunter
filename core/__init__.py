"""
core/
=====
DataSkew Hunter brain.

Modules:
  loader.py     → universal loader (CSV/JSON/Parquet/ORC/HDFS)
  profiler.py   → column analysis, hot key detection
  detector.py   → CoV/Gini calculations, skew severity
  corrector.py  → correction methods (salting, repartition, AQE, broadcast)
  recommender.py → recommendation engine
  analyzer.py   → end-to-end pipeline orchestrator
"""