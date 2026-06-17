"""
dashboard/views/results.py
==========================
Final analysis report with uniform indicators and stable PDF export.
"""

import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime
import tempfile
from fpdf import FPDF
import plotly.graph_objects as go
import matplotlib.pyplot as plt

def create_distribution_fig_plotly(sizes_before, sizes_after):
    """Create the Plotly figure for the interactive distribution chart on the dashboard."""
    fig = go.Figure()
    
    # Find the maximum number of partitions between before and after
    max_len = max(len(sizes_before), len(sizes_after))
    
    # Limit display for readability (e.g. Top 50)
    display_len = min(max_len, 50)
    
    # Pad with zeros if one array is shorter
    sb = sizes_before + [0] * (max_len - len(sizes_before))
    sa = sizes_after + [0] * (max_len - len(sizes_after))
    
    # Keep the original partition index (P0, P1...) and sort by the most loaded partitions before correction
    paired = list(enumerate(zip(sb, sa)))
    paired_sorted = sorted(paired, key=lambda x: x[1][0], reverse=True)
    
    # Keep only the top `display_len` partitions
    top_paired = paired_sorted[:display_len]
    
    sb_disp = [item[1][0] for item in top_paired]
    sa_disp = [item[1][1] for item in top_paired]
    
    x_axis = [f"P{item[0]}" for item in top_paired]
    
    fig.add_trace(go.Bar(
        x=x_axis, y=sb_disp,
        name="Before (Initial)",
        marker_color='#94A3B8',
        opacity=0.8
    ))
    
    fig.add_trace(go.Bar(
        x=x_axis, y=sa_disp,
        name="After (Optimized)",
        marker_color='#2563EB',
        opacity=0.9
    ))
    
    title = "Physical Distribution Impact"
    if max_len > 50:
        title += f" (Top 50 of {max_len} partitions)"

    fig.update_layout(
        title=title,
        barmode='group',
        template='plotly_white',
        plot_bgcolor='rgba(247,249,252,1)',
        paper_bgcolor='rgba(247,249,252,1)',
        xaxis=dict(showgrid=False, showline=True, linecolor='#E2E8F0', tickfont=dict(color='#475569')),
        yaxis=dict(showgrid=True, gridcolor='#E2E8F0', zeroline=False, tickfont=dict(color='#475569')),
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        height=400
    )
    return fig

def generate_pdf_report(report, before, after, improvement):
    """Generate the complete PDF report using Matplotlib for Docker stability."""
    try:
        pdf = FPDF()
        pdf.add_page()
        
        # Header
        pdf.set_font("Arial", "B", 20)
        pdf.set_text_color(37, 99, 235)
        pdf.cell(0, 20, "DATASKEW HUNTER AUDIT REPORT", ln=True, align="C")
        pdf.ln(10)
        
        # Summary
        pdf.set_font("Arial", "B", 14)
        pdf.set_text_color(15, 23, 42)
        pdf.cell(0, 10, "1. PERFORMANCE SUMMARY", ln=True)
        pdf.set_font("Arial", "", 11)
        pdf.cell(0, 8, f"Data source: {os.path.basename(report.get('path', 'N/A'))}", ln=True)
        pdf.cell(0, 8, f"Analysis key: {report.get('key_column', 'N/A')}", ln=True)
        pdf.cell(0, 8, f"Repartition gain: {improvement.get('cov_reduction_pct', 0):.1f}%", ln=True)
        pdf.ln(10)
        
        # Metrics table
        pdf.set_font("Arial", "B", 14)
        pdf.cell(0, 10, "2. METRIC DETAILS", ln=True)
        pdf.set_font("Arial", "B", 10)
        pdf.set_fill_color(241, 245, 249)
        pdf.cell(60, 10, "Metric", 1, 0, 'C', True)
        pdf.cell(60, 10, "Initial State", 1, 0, 'C', True)
        pdf.cell(60, 10, "Optimized State", 1, 1, 'C', True)
        
        pdf.set_font("Arial", "", 10)
        metrics = [
            ("Coef. Variation (CoV)", f"{before.get('cov', 0):.4f}", f"{after.get('cov', 0):.4f}"),
            ("Gini Index", f"{before.get('gini', 0):.4f}", f"{after.get('gini', 0):.4f}"),
            ("Max Partition Size", f"{before.get('max_size', 0):,}", f"{after.get('max_size', 0):,}")
        ]
        for m, b, a in metrics:
            pdf.cell(60, 10, m, 1)
            pdf.cell(60, 10, b, 1)
            pdf.cell(60, 10, a, 1, 1)
            
        # Static plot (Matplotlib) for the PDF
        pdf.ln(10)
        pdf.set_font("Arial", "B", 14)
        pdf.cell(0, 10, "3. COMPARED DISTRIBUTION (Top 50/100)", ln=True)
        
        sizes_before = before.get("partition_sizes", [])
        sizes_after = after.get("partition_sizes", [])
        
        max_len = max(len(sizes_before), len(sizes_after))
        display_len = min(max_len, 50) # Limit to 50 for the PDF otherwise it becomes unreadable
        
        sb = sizes_before + [0] * (max_len - len(sizes_before))
        sa = sizes_after + [0] * (max_len - len(sizes_after))
        
        paired = list(enumerate(zip(sb, sa)))
        paired_sorted = sorted(paired, key=lambda x: x[1][0], reverse=True)
        top_paired = paired_sorted[:display_len]
        
        plot_before = [item[1][0] for item in top_paired]
        plot_after = [item[1][1] for item in top_paired]
        labels = [f"P{item[0]}" for item in top_paired]
        
        plt.figure(figsize=(10, 5))
        x = range(display_len)
        plt.bar([i - 0.2 for i in x], plot_before, width=0.4, label='Initial (Data Skew)', color='#CBD5E1')
        plt.bar([i + 0.2 for i in x], plot_after, width=0.4, label='Optimized (DataSkew Hunter)', color='#2563EB')
        plt.xticks(x, labels, rotation='vertical', fontsize=8)
        plt.title("Impact of Data Redistribution (Before vs After)")
        plt.xlabel("Partition IDs (Sorted by initial size)")
        plt.ylabel("Number of Rows")
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        plt.legend()
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            plt.savefig(tmp.name)
            pdf.image(tmp.name, x=10, w=190)
            os.unlink(tmp.name)
        plt.close()
            
        return pdf.output(dest='S')
    except Exception as e:
        st.error(f"PDF generation error: {e}")
        return None

def render_results():
    st.markdown('<h1 style="font-size: 2.2rem; margin-bottom:0.1rem;">Execution Report</h1>', unsafe_allow_html=True)
    st.markdown('<p style="color:#64748B; margin-bottom:2rem;">Final summary of data distribution after remediation.</p>', unsafe_allow_html=True)

    if not st.session_state.report:
        st.info("No analysis data available. Please load a dataset.")
        return

    report = st.session_state.report
    before = report.get("detection_before",{})
    after = report.get("detection_after",{})
    improvement = report.get("improvement",{})
    profile = report.get("profile", {})


    # UNIFORM METRIC GRID
    st.markdown("### Performance Summary")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f'''<div class="pro-card">
            <p>TOTAL ROWS</p>
            <h3>{profile.get("total_rows", 0):,}</h3>
        </div>''', unsafe_allow_html=True)
    
    with col2:
        st.markdown(f'''<div class="pro-card">
            <p>ANALYSIS KEY</p>
            <h3 style="color:#2563EB;">{report.get("key_column", "N/A")}</h3>
        </div>''', unsafe_allow_html=True)
        
    with col3:
        st.markdown(f'''<div class="pro-card">
            <p>FINAL SKEW (CoV)</p>
            <h3>{after.get("cov", 0):.3f}</h3>
        </div>''', unsafe_allow_html=True)
        
    with col4:
        st.markdown(f'''<div class="pro-card">
            <p>IMPROVEMENT</p>
            <h3 style="color:#059669;">+{improvement.get("cov_reduction_pct", 0):.1f}%</h3>
        </div>''', unsafe_allow_html=True)

    # PRE-GENERATE EXPORTS (so there is only one button)
    if st.session_state.get("export_report_id") != report.get("timestamp"):
        with st.spinner("Preparing exports..."):
            st.session_state["export_pdf_bytes"] = generate_pdf_report(report, before, after, improvement)
            st.session_state["export_json_str"] = json.dumps(report, indent=2, default=str)
            st.session_state["export_csv_str"] = pd.DataFrame({
                "Indicator": ["CoV", "Gini", "Max Partition"],
                "Initial": [before.get("cov"), before.get("gini"), before.get("max_size")],
                "Optimized": [after.get("cov"), after.get("gini"), after.get("max_size")]
            }).to_csv(index=False)
            st.session_state["export_report_id"] = report.get("timestamp")

    # VISUALISATION INTERACTIVE
    st.markdown("### Physical Distribution Impact", unsafe_allow_html=True)
    with st.container(border=True):
        sizes_before = before.get("partition_sizes", [])
        sizes_after = after.get("partition_sizes", [])
        if sizes_before and sizes_after:
            st.plotly_chart(create_distribution_fig_plotly(sizes_before, sizes_after), use_container_width=True)

    if report.get("duration_before") is not None and report.get("duration_after") is not None:
        st.markdown("### Impact sur la vitesse de traitement", unsafe_allow_html=True)
        
        gain_val = report.get('gain_pct', 0.0)
        gain_color = "#059669" if gain_val > 0 else "#DC2626"
        gain_bg = "#ecfdf5" if gain_val > 0 else "#fef2f2"
        gain_sign = "+" if gain_val > 0 else ""
        
        b1, b2, b3 = st.columns(3)
        with b1:
            st.markdown(f'''<div class="pro-card" style="border-left: 4px solid #94A3B8; background: linear-gradient(145deg, #ffffff, #f8fafc);">
                <p style="font-weight: 600; color: #64748B; font-size: 0.85rem; margin-bottom: 5px;">AVANT CORRECTION</p>
                <h2 style="color: #475569; margin: 0; font-size: 2rem;">{report.get('duration_before', 0.0):.2f} s</h2>
            </div>''', unsafe_allow_html=True)
        with b2:
            st.markdown(f'''<div class="pro-card" style="border-left: 4px solid #2563EB; background: linear-gradient(145deg, #ffffff, #eff6ff);">
                <p style="font-weight: 600; color: #2563EB; font-size: 0.85rem; margin-bottom: 5px;">APRÈS CORRECTION</p>
                <h2 style="color: #1D4ED8; margin: 0; font-size: 2rem;">{report.get('duration_after', 0.0):.2f} s</h2>
            </div>''', unsafe_allow_html=True)
        with b3:
            st.markdown(f'''<div class="pro-card" style="border-left: 4px solid {gain_color}; background: linear-gradient(145deg, #ffffff, {gain_bg});">
                <p style="font-weight: 600; color: {gain_color}; font-size: 0.85rem; margin-bottom: 5px;">GAIN DE PERFORMANCE</p>
                <h2 style="color: {gain_color}; margin: 0; font-size: 2rem;">{gain_sign}{gain_val:.1f} %</h2>
            </div>''', unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            "<div style='background-color: #F8FAFC; border-left: 3px solid #CBD5E1; padding: 12px 16px; color: #475569; font-size: 0.9em; border-radius: 0 8px 8px 0;'>"
            "<i>Benchmark d'agrégation réalisé sur le cluster virtuel. Le gain illustre l'élimination du goulot d'étranglement (bottleneck) lié au Data Skew.</i>"
            "</div><br>",
            unsafe_allow_html=True
        )

    # ACTIONS AND EXPORTS
    st.markdown("### Export Results", unsafe_allow_html=True)
    exp1, exp2, exp3 = st.columns(3)

    with exp1:
        with st.container(border=True):
            st.markdown("**Premium PDF Report**")
            st.caption("Stable file with charts.")
            if st.session_state["export_pdf_bytes"]:
                st.download_button(
                    label="Download PDF",
                    data=bytes(st.session_state["export_pdf_bytes"]),
                    file_name=f"DataSkew_Audit_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    type="primary"
                )

    with exp2:
        with st.container(border=True):
            st.markdown("**Raw JSON Data**")
            st.caption("Full JSON file.")
            if st.session_state["export_json_str"]:
                st.download_button(
                    label="Download JSON",
                    data=st.session_state["export_json_str"],
                    file_name=f"DataSkew_Audit_{datetime.now().strftime('%Y%m%d')}.json",
                    mime="application/json",
                    use_container_width=True,
                    type="primary"
                )

    with exp3:
        with st.container(border=True):
            st.markdown("**CSV Summary**")
            st.caption("Summary table of gains.")
            if st.session_state["export_csv_str"]:
                st.download_button(
                    label="Download CSV",
                    data=st.session_state["export_csv_str"],
                    file_name=f"DataSkew_Audit_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    type="primary"
                )

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("New Analysis", use_container_width=True):
        st.session_state.report = None
        st.session_state.page = "data"
        st.rerun()
