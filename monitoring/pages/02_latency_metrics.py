import time
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import requests

st.set_page_config(page_title="Latency Metrics", layout="wide")
st.title("⚡ API Latency Metrics")
st.caption("Response time distribution for /features/{user_id} endpoint")

from monitoring.config import config

n_calls = st.sidebar.slider("Benchmark calls", 10, 200, 50, 10)

if st.button("Run Latency Benchmark"):
    latencies = []
    errors = 0
    user_ids = [f"usr_{i:05d}" for i in range(1, n_calls + 1)]

    progress = st.progress(0)
    for i, uid in enumerate(user_ids):
        t0 = time.perf_counter()
        try:
            r = requests.get(f"{config.feature_store_api_url}/features/{uid}", timeout=2)
            latency_ms = (time.perf_counter() - t0) * 1000
            if r.status_code in (200, 404):
                latencies.append(latency_ms)
            else:
                errors += 1
        except Exception:
            errors += 1
        progress.progress((i + 1) / n_calls)

    if latencies:
        df = pd.DataFrame({"latency_ms": latencies})

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("p50 (ms)", f"{pd.Series(latencies).quantile(0.50):.1f}")
        col2.metric("p95 (ms)", f"{pd.Series(latencies).quantile(0.95):.1f}")
        col3.metric("p99 (ms)", f"{pd.Series(latencies).quantile(0.99):.1f}")
        col4.metric("Errors", errors)

        fig = px.histogram(
            df, x="latency_ms", nbins=20,
            title=f"Latency Distribution ({n_calls} requests)",
            labels={"latency_ms": "Latency (ms)"},
            color_discrete_sequence=["#34A853"],
        )
        fig.add_vline(x=50, line_dash="dash", line_color="red", annotation_text="Target SLA (50ms)")
        st.plotly_chart(fig, use_container_width=True)

        above_sla = (df["latency_ms"] > 50).sum()
        if above_sla > 0:
            st.warning(f"{above_sla}/{n_calls} requests exceeded 50ms SLA ({above_sla/n_calls*100:.1f}%)")
        else:
            st.success(f"All {n_calls} requests met the 50ms SLA ✓")
    else:
        st.error("No successful requests. Is the API running?")
else:
    st.info("Click 'Run Latency Benchmark' to measure API response times.")
