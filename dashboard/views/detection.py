"""
dashboard/pages/detection.py
=============================
Page 3: Skew detection.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

SEVERITY_META = {
    "none":     ("#059669", "Optimal", "Data distribution is uniform."),
    "low":      ("#2563EB", "Correct", "Low variance detected."),
    "medium":   ("#D97706", "Unbalanced", "Moderate skew detected."),
    "high":     ("#DC2626", "Critical", "Potential bottleneck detected."),
    "critical": ("#7F1D1D", "Severe Critical", "Extreme skew detected."),
}

def render_detection():
    st.markdown('<h1 class="main-title">Distribution Diagnostics</h1>', unsafe_allow_html=True)

    if not st.session_state.get("report"):
        st.info("No analysis data available. Please load a dataset.")
        return

    report  = st.session_state.report
    before  = report.get("detection_before", {})
    rec     = report.get("recommendation",  {})
    cov     = before.get("cov", 0)
    gini    = before.get("gini", 0)
    severity = before.get("severity", "none")

    color, sev_label, sev_desc = SEVERITY_META.get(severity, SEVERITY_META["none"])

    # Severity Header
    st.markdown(f"""
    <div class="pro-card" style="border-left: 4px solid {color};">
        <h3 style="margin:0; color:{color};">{sev_label}</h3>
        <p style="color:var(--text-secondary); margin-bottom:0;">{sev_desc}</p>
    </div>
    """, unsafe_allow_html=True)

    # Metrics
    m1, m2, m3, m4 = st.columns(4)

    with m1:
        st.markdown(f'<div class="pro-card"><p style="font-size:0.75rem; color:var(--text-secondary);">COVARIANCE (COV)</p><h3>{cov:.3f}</h3></div>', unsafe_allow_html=True)
    with m2:
        st.markdown(f'<div class="pro-card"><p style="font-size:0.75rem; color:var(--text-secondary);">COEFFICIENT GINI</p><h3>{gini:.3f}</h3></div>', unsafe_allow_html=True)
    with m3:
        ratio = before.get("max_ratio", 0)
        st.markdown(f'<div class="pro-card"><p style="font-size:0.75rem; color:var(--text-secondary);">MAX/MEAN RATIO</p><h3>{ratio:.1f}x</h3></div>', unsafe_allow_html=True)
    with m4:
        st.markdown(f'<div class="pro-card"><p style="font-size:0.75rem; color:var(--text-secondary);">PARTITIONS</p><h3>{before.get("num_partitions", 0)}</h3></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Two side-by-side charts ──────────────────────────────────────────
    col_left, col_right = st.columns(2)

    # Graph 1: Physical Distribution
    with col_left:
        st.subheader("Physical Distribution")
        sizes = before.get("partition_sizes", [])
        if sizes:
            df_parts = pd.DataFrame({
                "Partition": [f"P{i}" for i in range(len(sizes))],
                "Rows": sizes
            })
            mean_size = before.get("mean_size", 0)

            fig1 = px.bar(
                df_parts, x="Partition", y="Rows",
                template="plotly_white",
                color_discrete_sequence=["#64748B"],
                height=280
            )
            fig1.add_hline(y=mean_size, line_dash="dash", line_color="#EF4444")
            fig1.update_layout(margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("No partition data available.")

    # Graph 2: Distribution by candidate key value
    with col_right:
        st.subheader("Candidate Key Distribution")
        key_dist = before.get("key_distribution", {})
        if key_dist:
            df_keys = pd.DataFrame({
                "Value": list(key_dist.keys()),
                "Rows": list(key_dist.values())
            }).sort_values("Rows", ascending=False)

            fig2 = px.bar(
                df_keys, x="Value", y="Rows",
                template="plotly_white",
                color_discrete_sequence=["#3B82F6"],
                height=280
            )
            fig2.update_layout(margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No key distribution available.")

    # Recommendation
    st.subheader("Optimization Recommendation")
    st.markdown(f"""
    <div class="pro-card">
        <p style="font-weight: 600; color: var(--accent);">Recommended method: {rec.get('primary_method', 'N/A').upper()}</p>
        <p style="color: var(--text-secondary);">{rec.get('explanation', '')}</p>
    </div>
    """, unsafe_allow_html=True)

    _nav()

def _nav():
    pass