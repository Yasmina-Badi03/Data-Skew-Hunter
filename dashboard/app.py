import streamlit as st
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(
    page_title="DataSkew Hunter",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── CSS global ────────────────────────────────────────────────────────────────
st.markdown("""
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/tabler-icons.min.css">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">

<style>
:root {
    --bg: #ffffff;
    --surface: #ffffff;
    --surface-strong: #e2e8f0;
    --sidebar-bg: #f1f5f9;
    --text: #0f172a;
    --text-secondary: #475569;
    --accent: #2563EB;
    --primary: #2563EB;
    --success: #059669;
    --warning: #D97706;
    --danger: #DC2626;
    --secondary: #64748B;
}

body, .main, .block-container, section[data-testid="stAppViewContainer"] {
    background: var(--bg) !important;
    color: var(--text) !important;
}

/* ── Sidebar container ── */
section[data-testid="stSidebar"] {
    background: var(--sidebar-bg) !important;
    border-right: 1px solid #e2e8f0 !important;
    width: 280px !important;
    min-width: 280px !important;
}

section[data-testid="stSidebar"] > div:first-child {
    padding: 0 !important;
}

[data-testid="stSidebarNav"] { display: none !important; }
footer, header, #MainMenu { visibility: hidden !important; }

/* ── Footer fixed at the bottom of the sidebar ── */
.sidebar-footer-fixed {
    position: fixed;
    bottom: 0;
    left: 0;
    width: 280px;
    z-index: 999;
    background: var(--sidebar-bg);
}

/* ── Steps ── */
.step-container {
    padding: 90px 16px 80px 16px;
    font-family: 'DM Sans', sans-serif;
}

.step-item {
    display: flex;
    align-items: flex-start;
    position: relative;
    padding-bottom: 100px;
    cursor: pointer;
    transition: opacity 0.2s;
}

.step-item:last-child { padding-bottom: 8px; }
.step-item:hover { opacity: 0.95; }

.step-item::before {
    content: '';
    position: absolute;
    left: 15px;
    top: 34px;
    bottom: 2px;
    width: 2px;
    background: #cbd5e1;
    z-index: 0;
}

.step-item:last-child::before { display: none; }

.step-icon {
    width: 32px;
    height: 32px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1;
    margin-right: 12px;
    flex-shrink: 0;
    transition: all 0.3s ease;
    border: 2px solid transparent;
}

.step-text { padding-top: 4px; }

.step-title {
    font-size: 17px;
    font-weight: 500;
    color: #334155;
    transition: color 0.3s ease;
    margin-bottom: 4px;
}

.step-desc {
    font-size: 13.5px;
    color: #64748B;
    font-weight: 400;
}

.step-item.completed .step-icon  { background:rgba(16,185,129,0.14); border-color:#10b981; color:#10b981; }
.step-item.completed .step-title { color:#10b981; font-weight:600; }
.step-item.completed .step-desc  { color:rgba(16,185,129,0.8); }

.step-item.active .step-icon  { background:rgba(37,99,235,0.12); border-color:#2563EB; color:#2563EB; box-shadow:0 0 0 4px rgba(37,99,235,0.08); }
.step-item.active .step-title { color:#2563EB; font-weight:600; }
.step-item.active .step-desc  { color:rgba(37,99,235,0.7); }

.step-item.pending .step-icon  { background:#f1f5f9; border-color:#cbd5e1; color:#64748B; }
.step-item.pending .step-title { color:#475569; }
.step-item.pending .step-desc  { color:#94a3b8; }

/* ── Brand ── */
.sidebar-brand-fixed {
    position: fixed;
    top: 0;
    left: 0;
    width: 280px;
    z-index: 999;
    background: var(--sidebar-bg);
}
.brand-block {
    padding: 18px 16px 16px;
    border-bottom: 1px solid #e2e8f0;
    display: flex;
    align-items: center;
    gap: 11px;
    font-family: 'DM Sans', sans-serif;
    background: var(--sidebar-bg);
}
.brand-icon {
    width:34px; height:34px; border-radius:9px; flex-shrink:0;
    background: linear-gradient(135deg,#2563EB,#7DD3FC);
    display:flex; align-items:center; justify-content:center;
    box-shadow: 0 4px 14px rgba(37,99,235,0.18);
}

/* ── Footer ── */
.help-footer {
    border-top: 1px solid #e2e8f0;
    padding: 14px 12px;
    display: flex;
    align-items: center;
    gap: 12px;
    font-family: 'DM Sans', sans-serif;
    background: var(--sidebar-bg);
}
.help-block {
    display: block;
    text-decoration: none;
}
.help-block:hover .help-footer {
    background: #e2e8f0;
    transition: background 0.2s ease;
}

/* ── Cards ── */
.pro-card, .glass-card {
    background: var(--surface);
    border: 1px solid #e2e8f0;
    border-radius: 20px;
    padding: 18px;
    box-shadow: 0 18px 50px rgba(15,23,42,0.06);
    color: var(--text);
}
.subtitle {
    margin: 0 0 8px;
    color: var(--text-secondary);
    font-size: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
.gradient-text {
    background: linear-gradient(135deg,#2563EB 0%,#0EA5E9 100%);
    -webkit-background-clip: text;
    color: transparent;
}
</style>
""", unsafe_allow_html=True)


# ─ Session state ─────────────────────────────────────────────────────────────
def init_session_state():
    defaults = {
        "page":          "data",
        "report":        None,
        "mode":          "auto",
        "key_column":    None,
        "method":        None,
        "salt_factor":   10,
        "analysis_done": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session_state()

# ─ Navigation via URL Params ──────────────────────────────────────────────────
if hasattr(st, 'query_params'):
    query_params = st.query_params
    if 'page' in query_params:
        new_page = query_params['page']
        if new_page in ["data", "audit", "results"]:
            if st.session_state.page != new_page:
                st.session_state.page = new_page
                del st.query_params['page']
                st.rerun()


# ─ Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        current_p = st.session_state.page

        # 1. Brand Header
        st.markdown("""
        <div class="sidebar-brand-fixed">
            <div class="brand-block">
                <div class="brand-icon">
                    <i class='ti ti-bolt' style='font-size:16px; color:#fff;'></i>
                </div>
                <div>
                    <div style="font-size:14px; font-weight:600; color:#0f172a; letter-spacing:-0.02em; line-height:1;">
                        DataSkew Hunter
                    </div>
                    <div style="font-size:11px; color:#475569; margin-top:3px; font-family:'JetBrains Mono',monospace;">
                        v1.0 · spark 3.x
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # 2. Steps
        steps = [
            {"id": "data",    "title": "Setup",        "desc": "Upload data",               "icon": "ti ti-upload"},
            {"id": "audit",   "title": "Detection",    "desc": "Skew analysis",            "icon": "ti ti-search"},
            {"id": "results", "title": "Report",       "desc": "Correction & results",      "icon": "ti ti-file-text"}
        ]

        try:
            active_index = [s["id"] for s in steps].index(current_p)
        except ValueError:
            active_index = 0

        # 3. Progress list HTML + JS dans un seul bloc
        progress_html = '<div class="step-container">'

        for i, step in enumerate(steps):
            if i < active_index:
                status      = "completed"
                color_icon  = "#10b981"
                bg_icon     = "rgba(16,185,129,0.1)"
                border_icon = "#10b981"
                text_title  = "#10b981"
                text_desc   = "rgba(16,185,129,0.7)"
            elif i == active_index:
                status      = "active"
                color_icon  = "#6366f1"
                bg_icon     = "rgba(99,102,241,0.15)"
                border_icon = "#6366f1"
                text_title  = "#a5b4fc"
                text_desc   = "rgba(165,180,252,0.7)"
            else:
                status      = "pending"
                color_icon  = "#595962"
                bg_icon     = "#59596284"
                border_icon = "#595962"
                text_title  = "#595962C3"
                text_desc   = "#595962"

            progress_html += f'<div class="step-item {status}" data-page="{step["id"]}">'
            progress_html += f'<div class="step-icon" style="background:{bg_icon};border-color:{border_icon};color:{color_icon}">'
            progress_html += f'<i class="{step["icon"]}" style="font-size:16px;"></i>'
            progress_html += '</div>'
            progress_html += '<div class="step-text">'
            progress_html += f'<div class="step-title" style="color:{text_title}">{step["title"]}</div>'
            progress_html += f'<div class="step-desc" style="color:{text_desc}">{step["desc"]}</div>'
            progress_html += '</div></div>'

        progress_html += '</div>'

        progress_html += """
        <script>
            document.addEventListener('DOMContentLoaded', function() {
                document.querySelectorAll('.step-item').forEach(step => {
                    step.addEventListener('click', function() {
                        const pageId = this.getAttribute('data-page');
                        if (pageId) {
                            const url = new URL(window.location.href);
                            url.searchParams.set('page', pageId);
                            window.location.href = url.toString();
                        }
                    });
                });
            });
        </script>
        """

        st.markdown(progress_html, unsafe_allow_html=True)

        # 4. Footer — position: fixed en bas de la sidebar
        st.markdown("""
        <div class="sidebar-footer-fixed">
            <a class="help-block" href="https://github.com/Yasmina-Badi03/Data-Skew-Hunter" target="_blank">
                <div class="help-footer">
                    <div style="
                        width:30px; height:30px; border-radius:8px; flex-shrink:0;
                        background:rgba(37,99,235,0.12);
                        display:flex; align-items:center; justify-content:center;
                        font-size:16px; font-weight:700; color:#2563EB;
                    ">?</div>
                    <div>
                        <div style="font-size:12.5px; font-weight:600; color:#0f172a; line-height:1.1;">
                            Help &amp; documentation
                        </div>
                    </div>
                </div>
            </a>
        </div>
        """, unsafe_allow_html=True)


render_sidebar()

# ── Routing ───────────────────────────────────────────────────────────────────
current_page = st.session_state.page

if current_page == "data":
    try:
        try:
            from dashboard.views.upload import render_upload
        except ImportError:
            from views.upload import render_upload
        render_upload()
    except Exception as e:
        st.title("Configuration")
        st.error(f"Error: {str(e)[:200]}")
        with st.expander("Details"):
            st.code(str(e))

elif current_page == "audit":
    try:
        try:
            from dashboard.views.audit import render_audit
        except ImportError:
            from views.audit import render_audit
        render_audit()
    except Exception as e:
        st.title("Audit / Detection")
        st.error(f"Error: {str(e)[:200]}")
        with st.expander("Details"):
            st.code(str(e))

elif current_page == "results":
    try:
        try:
            from dashboard.views.results import render_results
        except ImportError:
            from views.results import render_results
        render_results()
    except Exception as e:
        st.title("Results / Correction")
        st.error(f"Error: {str(e)[:200]}")
        with st.expander("Details"):
            st.code(str(e))

else:
    st.title("Page Not Found")
    st.error(f"The page '{current_page}' is not defined.")