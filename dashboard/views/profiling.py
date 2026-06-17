"""
dashboard/pages/profiling.py
============================
Page 2: Dataset profiling.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def render_profiling():
    st.markdown('<h1 class="gradient-text">Profiling & Distribution</h1>', unsafe_allow_html=True)

    if not st.session_state.get("report"):
        st.warning("No analysis available. Return to **Upload & Config** first.")
        if st.button("← Retour"):
            st.session_state.page = "upload"
            st.rerun()
        return

    report  = st.session_state.report
    profile = report.get("profile", {})
    key_col = report.get("key_column", "N/A")

    # ── KPIs ───────────────────────────────────────────────────────
    st.markdown("### Overview")
    k1, k2, k3, k4 = st.columns(4)
    
    with k1:
        st.markdown(f'<div class="glass-card"><p class="subtitle">ROWS</p><h2 style="color:var(--primary);">{profile.get("total_rows", 0):,}</h2></div>', unsafe_allow_html=True)
    with k2:
        st.markdown(f'<div class="glass-card"><p class="subtitle">COLUMNS</p><h2 style="color:var(--primary);">{profile.get("num_cols", 0)}</h2></div>', unsafe_allow_html=True)
    with k3:
        st.markdown(f'<div class="glass-card"><p class="subtitle">SKEW CANDIDATES</p><h2 style="color:var(--accent);">{len(profile.get("candidates", []))}</h2></div>', unsafe_allow_html=True)
    with k4:
        st.markdown(f'<div class="glass-card"><p class="subtitle">ACTIVE KEY</p><h2 style="color:var(--success);">{key_col or "auto"}</h2></div>', unsafe_allow_html=True)

    st.markdown("---")

    # ── Candidates ───────────────────────────────────────────────────
    st.markdown("### 🎯 Critical columns (Skew candidates)")

    candidates = profile.get("candidates", [])
    if not candidates:
        st.info("Uniform distribution detected across all columns.")
    else:
        df_cand = pd.DataFrame(candidates).sort_values("hot_key_ratio", ascending=False)
        
        c_chart, c_table = st.columns([1, 1])
        
        with c_chart:
            fig = px.bar(
                df_cand.head(10),
                x="hot_key_ratio",
                y="column",
                orientation="h",
                color="hot_key_ratio",
                color_continuous_scale=["#2563EB", "#60A5FA"],
                template="plotly_white",
                labels={"hot_key_ratio": "Max Freq (%)", "column": "Column"}
            )
            fig.update_layout(
                paper_bgcolor='rgba(247,249,252,1)',
                plot_bgcolor='rgba(247,249,252,1)',
                xaxis=dict(showgrid=False, showline=True, linecolor='#E2E8F0', tickfont=dict(color='#475569')),
                yaxis=dict(showgrid=True, gridcolor='#E2E8F0', zeroline=False, tickfont=dict(color='#475569')),
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
            
        with c_table:
            st.dataframe(df_cand, use_container_width=True)

    st.markdown("---")

    # ── Focus on Key ────────────────────────────────────────────────────
    key_analysis = report.get("detection_before", {}).get("key_analysis", {})

    if key_analysis and key_analysis.get("top_distribution"):
        st.markdown(f"### 🛡️ Focus: `{key_col}` Distribution")

        col1, col2 = st.columns([2, 1])

        with col1:
            top_dist = key_analysis["top_distribution"]
            df_dist  = pd.DataFrame(top_dist)
            
            fig2 = go.Figure(go.Bar(
                x=df_dist["value"],
                y=df_dist["pct"],
                marker=dict(
                    color=df_dist["pct"],
                    colorscale="Viridis",
                    line=dict(color="white", width=0.5)
                ),
                text=df_dist["pct"].apply(lambda v: f"{v:.1f}%"),
                textposition="outside"
            ))
            fig2.update_layout(
                paper_bgcolor='rgba(247,249,252,1)',
                plot_bgcolor='rgba(247,249,252,1)',
                template='plotly_white',
                height=450,
                xaxis_title='Key Value',
                yaxis_title='Dataset percentage',
                xaxis=dict(showgrid=False, showline=True, linecolor='#E2E8F0', tickfont=dict(color='#475569')),
                yaxis=dict(showgrid=True, gridcolor='#E2E8F0', zeroline=False, tickfont=dict(color='#475569'))
            )
            fig2.add_hline(y=5, line_dash='dash', line_color='#F472B6', annotation_text='HOT KEY THRESHOLD (5%)', annotation_position='top left')
            st.plotly_chart(fig2, use_container_width=True)

        with col2:
            st.markdown("#### Hot Keys Matrix")
            hot_keys = key_analysis.get("hot_keys", [])
            for hk in hot_keys:
                st.markdown(f"""
                <div style='background:rgba(239, 68, 68, 0.1); border:1px solid var(--danger); 
                            border-radius:12px; padding:15px; margin-bottom:10px;'>
                    <div style='color:var(--danger); font-weight:800; font-size:1.1rem;'>{hk['value']}</div>
                    <div style='display:flex; justify-content:space-between; margin-top:5px;'>
                        <span style='color:var(--secondary);'>{hk['count']:,} rows</span>
                        <span style='color:var(--text); font-weight:bold;'>{hk['percentage']:.2f}%</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            if not hot_keys:
                st.success("Clean Key: No Hot Keys Found")

    _nav()

def _nav():
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Settings"):
            st.session_state.page = "upload"
            st.rerun()
    with col2:
        if st.button("Detection →"):
            st.session_state.page = "detection"
            st.rerun()