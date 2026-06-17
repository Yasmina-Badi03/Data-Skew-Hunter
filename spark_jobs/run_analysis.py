"""
spark_jobs/run_analysis.py
==========================
Main Spark job for DataSkew Hunter.

Runs the full analysis and correction pipeline on any dataset.
This file is intended to be submitted using spark-submit.

Usage from the Spark container:
    # Automatic mode:
    spark-submit spark_jobs/run_analysis.py \
        --path hdfs://namenode:9000/data/ecommerce

    # Expert mode:
    spark-submit spark_jobs/run_analysis.py \
        --path hdfs://namenode:9000/data/ecommerce \
        --format parquet \
        --key product_id \
        --method salting \
        --salt-factor 10

    # Local CSV dataset:
    spark-submit spark_jobs/run_analysis.py \
        --path /data/transactions.csv \
        --format csv \
        --key user_id
"""

import argparse
import json
import sys
import os

from pyspark.sql import SparkSession

# Add the project root directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.analyzer import run_full_analysis


# ─── Argument parsing ───────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="DataSkew Hunter — Analyze and correct Spark data skew"
    )

    parser.add_argument(
        "--path", required=True,
        help="Path to the dataset (local or HDFS). Ex: hdfs://namenode:9000/data/file.parquet"
    )
    parser.add_argument(
        "--format", dest="fmt", default=None,
        choices=["csv", "json", "parquet", "orc"],
        help="File format (default: auto-detected from the extension)"
    )
    parser.add_argument(
        "--key", dest="key_column", default=None,
        help="Key column to analyze. If absent, auto-selected."
    )
    parser.add_argument(
        "--method", dest="correction_method", default=None,
        choices=["salting", "repartition", "broadcast", "aqe", "none"],
        help="Correction method. If absent, auto-recommended."
    )
    parser.add_argument(
        "--salt-factor", dest="salt_factor", type=int, default=10,
        help="Salt factor (default: 10). Higher = better dispersion."
    )
    parser.add_argument(
        "--partitions", dest="num_partitions", type=int, default=None,
        help="Target number of partitions for repartitioning."
    )
    parser.add_argument(
        "--sample", dest="sample_fraction", type=float, default=1.0,
        help="Sample fraction for profiling (1.0 = all, 0.1 = 10%)."
    )
    parser.add_argument(
        "--hot-threshold", dest="hot_threshold_pct", type=float, default=5.0,
        help="Threshold (%) to identify a hot key (default: 5.0)."
    )
    parser.add_argument(
        "--output", dest="output_path", default="results/report.json",
        help="JSON output file (default: results/report.json)"
    )
    parser.add_argument(
        "--master", dest="master", default="spark://spark-master:7077",
        help="Spark master URL"
    )

    return parser.parse_args()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    print("=" * 60)
    print("     DataSkew Hunter — Analysis Pipeline")
    print("=" * 60)
    print(f"  Path    : {args.path}")
    print(f"  Format  : {args.fmt or 'auto'}")
    print(f"  Key     : {args.key_column or 'auto'}")
    print(f"  Method  : {args.correction_method or 'auto'}")
    print(f"  Mode    : {'expert' if args.key_column else 'automatic'}")
    print("=" * 60)

    # ── Create the SparkSession ─────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("DataSkewHunter")
        .master(args.master)
        # AQE for Spark 3+
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # General optimizations
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ── Run the full analysis ──────────────────────────────────────────────
    report = run_full_analysis(
        spark=spark,
        path=args.path,
        fmt=args.fmt,
        key_column=args.key_column,
        correction_method=args.correction_method,
        salt_factor=args.salt_factor,
        num_partitions=args.num_partitions,
        sample_fraction=args.sample_fraction,
        hot_threshold_pct=args.hot_threshold_pct,
        output_path=args.output_path
    )

    # ── Display the summary ─────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("     RESULTS")
    print("=" * 60)
    print(f"  Dataset           : {report['dataset']['row_count']} rows")
    print(f"  Key analyzed      : {report.get('key_column', 'N/A')}")
    print(f"  Skew detected     : {report['detection_before']['skew']}")
    print(f"  CoV before        : {report['detection_before']['cov']:.3f}")
    print(f"  CoV after         : {report['detection_after']['cov']:.3f}")
    print(f"  CoV improvement   : {report['improvement']['cov_reduction_pct']:.1f}%")
    print(f"  Method applied    : {report['correction'].get('method', 'none')}")
    print(f"  Total duration    : {report['total_duration_sec']}s")
    print(f"  JSON report       : {args.output_path}")
    print("=" * 60)

    spark.stop()
    return report


if __name__ == "__main__":
    main()