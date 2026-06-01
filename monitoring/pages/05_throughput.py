import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import time

st.set_page_config(page_title="Throughput", layout="wide")
st.title("📈 Throughput & Pipeline Health")
st.caption("Event ingestion rates, Kafka lag, and Spark processing metrics")

from monitoring.data_fetcher import fetch_throughput_metrics, fetch_api_health, get_redis

# ── Current metrics ───────────────────────────────────────────────────────────
metrics = fetch_throughput_metrics()
health = fetch_api_health()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Active Users in Redis", f"{metrics['active_users']:,}")
col2.metric("Est. Events/sec", f"~{metrics['estimated_events_per_sec']:.0f}")
col3.metric("Redis Feature Keys", f"{metrics['redis_keys']:,}")
col4.metric("API Status", health.get("status", "unknown").upper())

st.divider()

# ── Feature value distribution across users ───────────────────────────────────
st.subheader("Current Feature Value Summary")
client = get_redis()
user_ids = client.srandmember("index:active_users", 300)

feature_stats = {}
FEATURES = ["txn_count_5m", "avg_spend_1h", "session_activity_rate",
            "cart_abandon_ratio", "product_interaction_freq", "anomaly_score"]

for uid in user_ids:
    data = client.hgetall(f"features:{uid}")
    for f in FEATURES:
        if f in data:
            try:
                feature_stats.setdefault(f, []).append(float(data[f]))
            except ValueError:
                pass

if feature_stats:
    rows = []
    for feat, vals in feature_stats.items():
        import numpy as np
        arr = np.array(vals)
        rows.append({
            "Feature": feat.replace("_", " ").title(),
            "Count": len(arr),
            "Mean": round(float(arr.mean()), 3),
            "Std": round(float(arr.std()), 3),
            "P50": round(float(np.percentile(arr, 50)), 3),
            "P95": round(float(np.percentile(arr, 95)), 3),
            "Max": round(float(arr.max()), 3),
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True)

    # Box plot
    import plotly.express as px
    all_vals = []
    for feat, vals in feature_stats.items():
        for v in vals[:100]:
            all_vals.append({"feature": feat.replace("_", " ").title(), "value": v})

    if all_vals:
        box_df = pd.DataFrame(all_vals)
        fig = px.box(
            box_df, x="feature", y="value",
            title="Feature Value Distribution (current)",
            color="feature",
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No feature data in Redis yet. Is the streaming pipeline running?")

# ── Pipeline component status ─────────────────────────────────────────────────
st.divider()
st.subheader("Pipeline Component Status")

components = {
    "Redis": ("🟢", "Connected") if health.get("redis") == "ok" else ("🔴", "Error"),
    "PostgreSQL": ("🟢", "Connected") if health.get("db") == "ok" else ("🔴", "Error"),
    "FastAPI": ("🟢", "Healthy") if health.get("status") == "healthy" else ("🔴", "Error"),
    "Kafka": ("🟡", "Not monitored directly"),
    "Spark": ("🟡", "Check Spark UI :8888"),
    "Airflow": ("🟡", "Check Airflow UI :8080"),
}

cols = st.columns(3)
for i, (name, (icon, status)) in enumerate(components.items()):
    cols[i % 3].info(f"**{name}**: {icon} {status}")
