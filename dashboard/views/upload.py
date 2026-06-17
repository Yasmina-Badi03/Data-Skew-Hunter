"""
dashboard/views/upload.py
=========================
Handles ingestion and demo initialization with forced skew.
"""

import streamlit as st
import pandas as pd
import os
import sys
import tempfile
import time
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

@st.cache_resource
def get_spark_session(master_url):
    from pyspark.sql import SparkSession
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    try:
        spark = SparkSession.getActiveSession()
        if spark and (spark.sparkContext._jsc is None or spark.sparkContext._jsc.sc().isStopped()):
            spark.stop()
            spark = None
        if not spark:
            spark = (SparkSession.builder
                .appName("DataSkewHunter-Enterprise")
                .master(master_url)
                # DISABLE AQE: AQE dynamically coalesces small partitions.
                # This breaks the post-correction visualization because it merges
                # uniformly distributed partitions into a single large partition (P0).
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.driver.host", "dataskew-hunter-dashboard")
                .config("spark.rpc.message.maxSize", "1024")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
                .getOrCreate())
        return spark
    except Exception as e:
        st.error(f"Spark error: {e}")
        return None

def _copy_local_to_hdfs(spark, local_path, hdfs_dir="/data/uploads"):
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        hdfs_target_dir = jvm.org.apache.hadoop.fs.Path(hdfs_dir)
        if not fs.exists(hdfs_target_dir):
            fs.mkdirs(hdfs_target_dir)

        file_name = os.path.basename(local_path)
        hdfs_target = jvm.org.apache.hadoop.fs.Path(f"hdfs://namenode:9000{hdfs_dir}/{file_name}")
        local_path_obj = jvm.org.apache.hadoop.fs.Path(local_path)
        fs.copyFromLocalFile(False, True, local_path_obj, hdfs_target)

        return hdfs_target.toString()
    except Exception as e:
        st.warning(f"Unable to copy file to HDFS: {e}")
        return None


def init_demo_data(spark, scenario):
    """Generate demo data with forced physical skew for visualization."""
    paths = {
        "heavy_skew": "hdfs://namenode:9000/data/heavy_skew",
        "medium_skew": "hdfs://namenode:9000/data/medium_skew",
        "no_skew": "hdfs://namenode:9000/data/no_skew"
    }
    target = paths.get(scenario)
    
    try:
        # Existence check
        spark.read.parquet(target).limit(1).collect()
        return target
    except:
        with st.spinner(f"Generating scenario: {scenario}..."):
            # 1. Create the dataset (500k rows)
            df = spark.range(0, 500000).withColumn("amount", (F.rand() * 100).cast("int"))
            
            if scenario == "heavy_skew":
                # 90% of data on 'HOT_USER_1'
                df = df.withColumn("user_id", 
                    F.when(F.col("id") < 450000, F.lit("HOT_USER_1"))
                    .otherwise(F.concat(F.lit("USER_"), F.col("id").cast("string")))
                )
            elif scenario == "medium_skew":
                # 50% on 'MID_USER_1'
                df = df.withColumn("user_id", 
                    F.when(F.col("id") < 250000, F.lit("MID_USER_1"))
                    .otherwise(F.concat(F.lit("USER_"), F.col("id").cast("string")))
                )
            else:
                df = df.withColumn("user_id", F.concat(F.lit("USER_"), F.col("id").cast("string")))

            # 2. Force the physical skew
            # Repartition by user_id. Since many rows share the same key,
            # Spark will place most data on a few partitions.
            # We request 16 partitions to illustrate partition imbalance.
            df.repartition(16, "user_id").write.mode("overwrite").parquet(target)
            return target

def render_upload():
    st.markdown('<h1 style="font-size: 2.2rem; margin-bottom:0.1rem;">Data Source</h1>', unsafe_allow_html=True)
    st.markdown('<p style="color:#64748B; margin-bottom:2rem;">Configure ingestion and launch the distribution audit.</p>', unsafe_allow_html=True)

    st.markdown("### 1. Source Selection")
    source_type = st.radio(
        "Source", ["Local upload", "HDFS infrastructure", "Demo scenarios"],
        index=2, horizontal=True, label_visibility="collapsed"
    )

    path, fmt, df_preview = None, None, None
    spark = get_spark_session(os.getenv("SPARK_MASTER", "local[*]"))

    with st.container(border=True):
        if "local" in source_type.lower():
            uploaded = st.file_uploader("File (CSV, JSON, PARQUET)", type=["csv", "json", "parquet"], label_visibility="collapsed")
            if uploaded:
                suffix = "." + uploaded.name.split(".")[-1]
                # Save to shared volume
                os.makedirs("/app/uploads", exist_ok=True)
                path = os.path.join("/app/uploads", f"upload_{int(time.time())}{suffix}")
                with open(path, "wb") as f:
                    f.write(uploaded.getbuffer())
                fmt = suffix[1:]
                try:
                    if fmt == "csv": df_preview = pd.read_csv(path, nrows=10)
                    elif fmt == "json": df_preview = pd.read_json(path, lines=True, nrows=10)
                    elif fmt == "parquet": df_preview = pd.read_parquet(path).head(10)
                except: pass

        elif "hdfs" in source_type.lower():
            c1, c2 = st.columns([3, 1])
            with c1: path = st.text_input("URI HDFS", placeholder="hdfs://namenode:9000/...")
            with c2: fmt = st.selectbox("Format", ["parquet", "csv", "json", "orc"])

        else:
            demo_map = {
                "E-commerce (Critical Skew 90%)": "heavy_skew",
                "Logistics (Medium Skew 50%)": "medium_skew",
                "Uniform Distribution": "no_skew"
            }
            choice = st.selectbox("Choose a scenario", list(demo_map.keys()))
            if spark:
                path = init_demo_data(spark, demo_map[choice])
                fmt = "parquet"

    if df_preview is not None:
        st.markdown("### 2. Quick Preview", unsafe_allow_html=True)
        st.dataframe(df_preview, use_container_width=True)

    st.markdown("### 3. Audit Settings", unsafe_allow_html=True)
    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            sample = st.slider("Sampling Rate (%)", 10, 100, 100) / 100.0
        with c2:
            threshold = st.select_slider("Skew detection threshold (%)", options=[1, 2, 5, 10, 20], value=5)

    if path:
        st.markdown("<br>", unsafe_allow_html=True)
        _, b_col, _ = st.columns([1, 1.5, 1])
        with b_col:
            if st.button("RUN INITIAL AUDIT", use_container_width=True, type="primary"):
                _run_initial_audit(path, fmt, sample, threshold)

def _run_initial_audit(local_path, fmt, sample, threshold):
    with st.spinner(" Running initial analysis on the cluster..."):
        try:
            spark = get_spark_session(os.getenv("SPARK_MASTER", "local[*]"))
            p, f = local_path, fmt

            # Ingest local files (uploaded files in /app/uploads shared with the workers)
            if "spark://" in os.getenv("SPARK_MASTER", "") and not local_path.startswith("hdfs://"):
                hdfs_uri = _copy_local_to_hdfs(spark, local_path)
                if hdfs_uri:
                    p = hdfs_uri
                else:
                    p = "file://" + local_path
                f = fmt
            else:
                p = local_path
                f = fmt

            from core.analyzer import run_initial_analysis
            report, df = run_initial_analysis(
                spark=spark, path=p, fmt=f,
                sample_fraction=sample,
                hot_threshold_pct=threshold
            )
            
            st.session_state.report = report
            # Store the df path instead of df itself to avoid session serialization issues, or we can just keep df if the session handles it.
            # Usually keeping Spark DF in session_state is okay as long as SparkSession is alive.
            st.session_state.df = df
            st.session_state.page = "audit"
            st.rerun()
        except Exception as e:
            st.error(f"Spark error: {e}")