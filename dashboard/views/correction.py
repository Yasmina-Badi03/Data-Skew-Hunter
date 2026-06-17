"""
dashboard/pages/correction.py
==============================
Page 4 : Correction du skew.
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

METHOD_META = {
    "salting": {
        "icon": "🧂", "title": "Salting Optimization", "color": "var(--primary)",
        "desc": "Redistributes hot keys by appending random noise (salt) to values, effectively dispersing high-frequency keys across multiple partitions."
    },
    "repartition": {
        "icon": "🔄", "title": "Global Repartition", "color": "var(--accent)",
        "desc": "Forces a full shuffle of the dataset to achieve a uniform distribution across the cluster. Best for moderate skew without specific hot keys."
    },
    "aqe": {
        "icon": "🤖", "title": "Adaptive Execution (AQE)", "color": "var(--success)",
        "desc": "Leverages Spark 3 runtime statistics to dynamically re-plan and optimize skewed joins and aggregations without code changes."
    },
    "broadcast": {
        "icon": "📡", "title": "Broadcast Join", "color": "var(--warning)",
        "desc": "Eliminates the big table shuffle by broadcasting the smaller table to all worker nodes. Extremely efficient for small dimension tables."
    },
    "none": {
        "icon": "✅", "title": "No Correction Applied", "color": "var(--secondary)",
        "desc": "Data distribution is within operational limits. Manual intervention is not recommended."
    }
}

def render_correction():
    st.markdown('<h1 class="gradient-text">Remediation & Optimization</h1>', unsafe_allow_html=True)

    if not st.session_state.get("report"):
        st.warning("⚠️ Aucune analyse disponible. Retournez à Upload & Config.")
        if st.button("← Retour"):
            st.session_state.page = "upload"
            st.rerun()
        return

    report     = st.session_state.report
    correction = report.get("correction", {})
    rec        = report.get("recommendation", {})
    before     = report.get("detection_before", {})
    after      = report.get("detection_after", {})

    method = correction.get("method", rec.get("primary_method", "none"))
    info   = METHOD_META.get(method, METHOD_META["none"])

    # ── Applied Strategy ───────────────────────────────────────────
    st.markdown(f"""
    <div class="glass-card" style="border-left: 5px solid {info['color']};">
        <h2 style="color:{info['color']}; margin-bottom: 5px;">{info['icon']} {info['title']}</h2>
        <p style="color:var(--text);">{info['desc']}</p>
    </div>
    """, unsafe_allow_html=True)

    if not correction.get("applied", False):
        st.info("Optimization engine skipped: conditions met for stable execution without further correction.")
        _nav()
        return

    # ── Parameters ─────────────────────────────────────────────────
    st.markdown("### Execution Parameters")
    p1, p2, p3 = st.columns(3)
    
    with p1:
        st.markdown(f'<div class="glass-card"><p class="subtitle">STRATEGY</p><h3 style="color:var(--primary);">{method.upper()}</h3></div>', unsafe_allow_html=True)
    with p2:
        st.markdown(f'<div class="glass-card"><p class="subtitle">LATENCY</p><h3 style="color:var(--success);">{correction.get("duration_sec", 0):.2f}s</h3></div>', unsafe_allow_html=True)
    with p3:
        if method == "salting":
            st.markdown(f'<div class="glass-card"><p class="subtitle">SALT FACTOR</p><h3 style="color:var(--accent);">{correction.get("salt_factor", 10)}X</h3></div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="glass-card"><p class="subtitle">INSTANCES</p><h3 style="color:var(--secondary);">{correction.get("num_partitions", "Auto")}</h3></div>', unsafe_allow_html=True)

    # ── Spark Code Gen ─────────────────────────────────────────────
    st.markdown("### 💻 Automated Spark Implementation")
    key_col = report.get("key_column", "key")
    hot_keys = correction.get("hot_keys", [])
    sf = correction.get("salt_factor", 10)
    
    if method == "salting":
        code = f"""# Salting Strategy Implementation for {key_col}
HOT_KEYS = {hot_keys}
SALT_FACTOR = {sf}

df_corrected = df.withColumn(
    "salted_key",
    F.when(F.col("{key_col}").isin(HOT_KEYS),
           F.concat(F.col("{key_col}"), F.lit("_"), (F.rand() * SALT_FACTOR).cast("int"))
    ).otherwise(F.col("{key_col}"))
)"""
    elif method == "repartition":
        code = f"""# Global Repartitioning
N_PARTITIONS = {correction.get('num_partitions', 200)}
df_corrected = df.repartition(N_PARTITIONS, "{key_col}")"""
    else:
        code = "# Standard execution path"

    st.code(code, language="python")

    # ── Performance Impact ─────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📈 Optimization Delta (Before vs After)")
    
    comp1, comp2 = st.columns(2)
    
    with comp1:
        _impact_chart(before.get("partition_sizes", []), "Distribution: Baseline", "var(--danger)")
    with comp2:
        _impact_chart(after.get("partition_sizes", []), "Distribution: Optimized", "var(--success)")

    # ── Improvements ──────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    improvement = report.get("improvement", {})
    if improvement:
        i1, i2, i3 = st.columns(3)
        with i1:
            st.markdown(f'<div class="glass-card" style="text-align:center;"><p class="subtitle">COV REDUCTION</p><h2 style="color:var(--success);">-{improvement.get("cov_reduction_pct", 0):.1f}%</h2></div>', unsafe_allow_html=True)
        with i2:
            st.markdown(f'<div class="glass-card" style="text-align:center;"><p class="subtitle">GINI REDUCTION</p><h2 style="color:var(--success);">-{improvement.get("gini_reduction_pct", 0):.1f}%</h2></div>', unsafe_allow_html=True)
        with i3:
            status = "SUCCESS" if improvement.get("improved", False) else "PARTIAL"
            st.markdown(f'<div class="glass-card" style="text-align:center;"><p class="subtitle">STATUS</p><h2 style="color:var(--primary);">{status}</h2></div>', unsafe_allow_html=True)

    _nav()

def _impact_chart(sizes, title, color):
    labels = [f"P{i}" for i in range(len(sizes))]
    fig = go.Figure(go.Bar(
        x=labels, y=sizes,
        marker_color=color,
        opacity=0.85
    ))
    fig.update_layout(
        title=title,
        template='plotly_white',
        paper_bgcolor='rgba(247,249,252,1)', plot_bgcolor='rgba(247,249,252,1)',
        xaxis=dict(showgrid=False, showline=True, linecolor='#E2E8F0', tickfont=dict(color='#475569')),
        yaxis=dict(showgrid=True, gridcolor='#E2E8F0', zeroline=False, tickfont=dict(color='#475569')),
        height=300, showlegend=False, margin=dict(t=40, b=20, l=40, r=20)
    )
    st.plotly_chart(fig, use_container_width=True)

def _nav():
    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Detection Diagnostics"):
            st.session_state.page = "detection"
            st.rerun()
    with col2:
        if st.button("Final Report →"):
            st.session_state.page = "results"
            st.rerun()