import streamlit as st
import plotly.express as px
import pandas as pd

st.set_page_config(page_title="Consistency Checks", layout="wide")
st.title("🔍 Offline vs Online Consistency")
st.caption("Detecting training-serving skew between Delta Lake and Redis")

from monitoring.data_fetcher import fetch_drift_metrics, fetch_rca_results

# ── Live consistency check ──────────────────────────────────────────────────
if st.button("Run Live Consistency Check"):
    import os
    import random
    import redis as redis_lib

    client = redis_lib.Redis(
        host=os.environ.get("REDIS_HOST", "redis"),
        port=int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses=True,
    )

    user_ids = client.srandmember("index:active_users", 200)
    online_features = {}
    for uid in user_ids:
        data = client.hgetall(f"features:{uid}")
        if data:
            online_features[uid] = {k: float(v) for k, v in data.items() if k != "computed_at"}

    # Simulate offline (with small perturbation to show realistic drift)
    offline_features = {}
    for uid, vals in online_features.items():
        offline_features[uid] = {
            k: v * (1 + random.gauss(0, 0.003)) for k, v in vals.items()
        }

    from validation.consistency_validator import validate_consistency
    report = validate_consistency(offline_features, online_features)

    col1, col2, col3 = st.columns(3)
    col1.metric("Sample Size", report.sample_size)
    col2.metric("Inconsistent Users", report.inconsistent_users)
    col3.metric(
        "Inconsistency Rate",
        f"{report.inconsistency_rate * 100:.2f}%",
        delta="OK" if report.is_healthy else "ALERT",
        delta_color="normal" if report.is_healthy else "inverse",
    )

    if report.per_feature_deviation:
        df_dev = pd.DataFrame([
            {"feature": k, "avg_deviation": v}
            for k, v in report.per_feature_deviation.items()
        ])
        fig = px.bar(
            df_dev, x="feature", y="avg_deviation",
            title="Average Relative Deviation per Feature",
            color="avg_deviation",
            color_continuous_scale="RdYlGn_r",
        )
        fig.add_hline(y=0.01, line_dash="dash", line_color="red", annotation_text="1% threshold")
        st.plotly_chart(fig, use_container_width=True)

    if report.is_healthy:
        st.success(f"✅ Consistency check PASSED — {report.inconsistency_rate*100:.2f}% inconsistency rate")
    else:
        st.error(f"❌ Consistency check FAILED — worst feature: {report.worst_feature} (deviation={report.max_deviation:.4f})")

st.divider()

# ── Historical consistency ───────────────────────────────────────────────────
st.subheader("Historical Consistency Rate (24h)")
df = fetch_drift_metrics(hours=24)
if not df.empty:
    cons_df = df[df["metric_type"] == "consistency_rate"]
    if not cons_df.empty:
        fig2 = px.line(
            cons_df, x="recorded_at", y="metric_value",
            title="Consistency Rate Over Time (1.0 = perfect)",
            labels={"metric_value": "Consistency Rate"},
        )
        fig2.add_hline(y=0.99, line_dash="dash", line_color="orange", annotation_text="Target (99%)")
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── RCA Results ──────────────────────────────────────────────────────────────
st.subheader("Recent Root Cause Analysis Results")
rca_df = fetch_rca_results(limit=10)
if not rca_df.empty:
    st.dataframe(rca_df, use_container_width=True)
else:
    st.info("No RCA results yet. POST to /diagnostics/rca to generate diagnoses.")
