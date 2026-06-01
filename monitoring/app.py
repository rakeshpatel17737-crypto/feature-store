"""Streamlit monitoring dashboard entry point."""
import streamlit as st

st.set_page_config(
    page_title="Feature Store Monitor",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded",
)

from monitoring.data_fetcher import fetch_api_health, fetch_active_user_count, fetch_throughput_metrics
import plotly.graph_objects as go

# ── Header ───────────────────────────────────────────────────────────────────
st.title("🏪 Enterprise Feature Store — Live Monitor")
st.caption("Real-time observability for the ML Feature Store pipeline")

# ── System Health ─────────────────────────────────────────────────────────────
health = fetch_api_health()
col1, col2, col3, col4 = st.columns(4)

with col1:
    status = health.get("status", "unknown")
    color = "🟢" if status == "healthy" else "🔴"
    st.metric("API Status", f"{color} {status.upper()}")

with col2:
    redis_status = health.get("redis", "unknown")
    color = "🟢" if redis_status == "ok" else "🔴"
    st.metric("Redis", f"{color} {redis_status.upper()}")

with col3:
    db_status = health.get("db", "unknown")
    color = "🟢" if db_status == "ok" else "🔴"
    st.metric("PostgreSQL", f"{color} {db_status.upper()}")

with col4:
    active_users = fetch_active_user_count()
    st.metric("Active Users (Redis)", f"{active_users:,}")

st.divider()

# ── Quick Feature Lookup ──────────────────────────────────────────────────────
st.subheader("Quick Feature Lookup")
col_input, col_btn = st.columns([3, 1])
with col_input:
    lookup_user = st.text_input("User ID", value="usr_00001", label_visibility="collapsed")
with col_btn:
    lookup = st.button("Fetch Features")

if lookup and lookup_user:
    from monitoring.data_fetcher import fetch_feature_sample
    import time

    t0 = time.perf_counter()
    result = fetch_feature_sample(lookup_user)
    latency_ms = (time.perf_counter() - t0) * 1000

    if result and "features" in result:
        st.success(f"Retrieved in **{latency_ms:.1f}ms** (target: < 50ms)")
        features = result["features"]
        meta = result.get("metadata", {})

        cols = st.columns(3)
        feature_items = list(features.items())
        for i, (k, v) in enumerate(feature_items):
            cols[i % 3].metric(k.replace("_", " ").title(), f"{float(v):.3f}")

        if meta.get("freshness_seconds"):
            st.caption(f"Feature age: {meta['freshness_seconds']:.1f}s | Computed at: {meta.get('computed_at', 'N/A')}")
    else:
        st.warning(f"No features found for {lookup_user}. Is the event generator running?")

st.divider()

# ── Navigation hint ───────────────────────────────────────────────────────────
st.info(
    "📊 Use the **sidebar** to navigate to detailed pages:\n"
    "- **Feature Freshness** — Age distribution of cached features\n"
    "- **Latency Metrics** — API response time distributions\n"
    "- **Drift Report** — PSI and KS test results per feature\n"
    "- **Consistency Checks** — Offline vs Online comparison\n"
    "- **Throughput** — Event ingestion rates"
)

# ── Auto-refresh ──────────────────────────────────────────────────────────────
st.sidebar.title("⚙️ Settings")
auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
if auto_refresh:
    import time
    time.sleep(30)
    st.rerun()
