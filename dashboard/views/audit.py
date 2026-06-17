"""
dashboard/views/audit.py
========================
Initial audit page for visualizing skew metrics and choosing the correction method.
"""

import streamlit as st
import plotly.graph_objects as go
from core.analyzer import apply_correction_and_evaluate

def create_initial_distribution_fig(sizes):
    fig = go.Figure()
    # Display up to 50 partitions to keep it readable
    max_display = min(len(sizes), 50)
    display_sizes = sizes[:max_display]
    x_axis = [f"P{i}" for i in range(max_display)]
    
    fig.add_trace(go.Bar(
        x=x_axis, y=display_sizes,
        name="Current Distribution",
        marker_color='#94A3B8',
        opacity=0.8
    ))
    
    fig.update_layout(
        title=f"Initial Physical Distribution (Top {max_display} partitions)" if len(sizes) > 50 else "Initial Physical Distribution",
        template='plotly_white',
        plot_bgcolor='rgba(247,249,252,1)',
        paper_bgcolor='rgba(247,249,252,1)',
        xaxis=dict(showgrid=False, showline=True, linecolor='#E2E8F0', tickfont=dict(color='#475569')),
        yaxis=dict(showgrid=True, gridcolor='#E2E8F0', zeroline=False, tickfont=dict(color='#475569')),
        margin=dict(l=20, r=20, t=40, b=20),
        height=380
    )
    return fig


def create_key_distribution_fig(top_distribution):
    fig = go.Figure()
    values = [item['value'] for item in top_distribution]
    counts = [item['count'] for item in top_distribution]
    percents = [item['pct'] for item in top_distribution]

    fig.add_trace(go.Bar(
        x=values,
        y=counts,
        text=[f"{p:.1f}%" for p in percents],
        textposition='outside',
        marker_color='#2563EB',
        opacity=0.9
    ))

    fig.update_layout(
        title='Candidate Key Value Distribution',
        xaxis_title='Key Value',
        yaxis_title='Number of Rows',
        template='plotly_white',
        plot_bgcolor='rgba(247,249,252,1)',
        paper_bgcolor='rgba(247,249,252,1)',
        xaxis=dict(showgrid=False, showline=True, linecolor='#E2E8F0', tickfont=dict(color='#475569')),
        yaxis=dict(showgrid=True, gridcolor='#E2E8F0', zeroline=False, tickfont=dict(color='#475569')),
        margin=dict(l=20, r=20, t=40, b=20),
        height=380
    )
    return fig

def render_audit():
    st.markdown('<h1 style="font-size: 2.2rem; margin-bottom:0.1rem;">Audit & Metrics</h1>', unsafe_allow_html=True)
    st.markdown('<p style="color:#64748B; margin-bottom:2rem;">Analyze your data distribution before choosing a correction.</p>', unsafe_allow_html=True)

    if not st.session_state.report or "detection_before" not in st.session_state.report:
        st.info("No initial analysis available. Please load a dataset from the configuration.")
        return

    report = st.session_state.report
    df = st.session_state.get("df")
    
    if df is None:
        st.error("The Spark DataFrame could not be retrieved. Please reload the data.")
        return

    dataset = report.get("dataset", {})
    detection = report.get("detection_before", {})
    key_analysis = detection.get("key_analysis", {})
    key_selection = report.get("auto_key_column", "N/A")
    recommendation = report.get("recommendation", {})

    # 1. KPIs Initiaux
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f'''<div class="pro-card">
            <p>TOTAL ROWS</p>
            <h3>{dataset.get("row_count", 0):,}</h3>
        </div>''', unsafe_allow_html=True)
    with col2:
        st.markdown(f'''<div class="pro-card">
            <p>CANDIDATE KEY</p>
            <h3 style="color:#2563EB;">{key_selection}</h3>
        </div>''', unsafe_allow_html=True)
    with col3:
        severity_color = "#059669" if detection.get("severity") == "none" else "#D97706" if detection.get("severity") == "medium" else "#EF4444"
        st.markdown(f'''<div class="pro-card">
            <p>COEF. VARIATION (SKEW)</p>
            <h3 style="color:{severity_color};">{detection.get("cov", 0):.3f}</h3>
        </div>''', unsafe_allow_html=True)
    with col4:
        st.markdown(f'''<div class="pro-card">
            <p>RECOMMENDATION</p>
            <h3>{recommendation.get("primary_method", "N/A").upper()}</h3>
        </div>''', unsafe_allow_html=True)

    st.markdown("<br><br>", unsafe_allow_html=True)

    # 2. Charts side by side
    with st.container(border=True):
        left_col, right_col = st.columns(2)

        with left_col:
            
            sizes = detection.get("partition_sizes", [])
            note = detection.get("simulation_note") or "Current physical distribution of the loaded DataFrame."
            if sizes:
                st.plotly_chart(create_initial_distribution_fig(sizes), use_container_width=True)
                st.caption(note)
            else:
                st.info("No partition data available.")

        with right_col:
            top_distribution = key_analysis.get("top_distribution", [])
            if top_distribution:
                st.plotly_chart(create_key_distribution_fig(top_distribution), use_container_width=True)
                st.caption("Each bar represents the number of rows for a candidate key value.")
            else:
                st.info("No key distribution available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # 3. Hot Keys Details
    hot_keys = key_analysis.get("hot_keys", [])
    if hot_keys:
        with st.expander("Detected Hot Keys Details"):
            st.write(f"Analyzed key: **{key_analysis.get('key_column')}**")
            for hk in hot_keys:
                st.write(f"- Value: `{hk['value']}` -> **{hk['count']:,} rows** ({hk['percentage']:.1f}%)")

    # 4. Correction Form
    st.markdown("### Correction Configuration", unsafe_allow_html=True)
    with st.container(border=True):
        st.info("Choose the method to balance your data across the Spark cluster.")
        
        c1, c2 = st.columns(2)
        with c1:
            method = st.selectbox(
                "Remediation method", 
                ["auto", "salting", "repartition", "none"],
                index=0,
                help="'auto' will follow the AI recommendation."
            )
            key_col_input = st.text_input("Key column (leave blank for auto)", value="")
        
        with c2:
            salt_factor = st.number_input("Salting factor (if applicable)", min_value=1, max_value=100, value=10)
            num_part = st.number_input("Target partition count (for repartition)", min_value=0, max_value=2000, value=0, help="0 = default value")
            
        _, btn_col, _ = st.columns([1, 1.5, 1])
        with btn_col:
            if st.button("APPLY CORRECTION", type="primary", use_container_width=True):
                with st.spinner("Executing correction and computing gains..."):
                    try:
                        final_report = apply_correction_and_evaluate(
                            df=df,
                            report=report,
                            key_column=key_col_input if key_col_input else None,
                            correction_method=None if method == "auto" else method,
                            salt_factor=salt_factor,
                            num_partitions=num_part if num_part > 0 else None
                        )
                        st.session_state.report = final_report
                        st.session_state.page = "results"
                        st.rerun()
                    except Exception as e:
                        st.error(f"Correction error: {e}")
