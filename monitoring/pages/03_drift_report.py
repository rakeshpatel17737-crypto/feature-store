import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np

st.set_page_config(page_title="Drift Report", layout="wide")
st.title("📉 Feature Drift Report")
st.caption("PSI and KS test statistics per feature — detecting distribution shift")

from monitoring.data_fetcher import fetch_drift_metrics, get_redis

# ── Live PSI from Redis ────────────────────────────────────────────────────────
st.subheader("Current Drift Status")

FEATURES = ["txn_count_5m", "avg_spend_1h", "session_activity_rate",
            "cart_abandon_ratio", "product_interaction_freq", "anomaly_score"]

client = get_redis()

cols = st.columns(3)
for i, feature in enumerate(FEATURES):
    vals = []
    for uid in client.srandmember("index:active_users", 100):
        data = client.hgetall(f"features:{uid}")
        if feature in data:
            try:
                vals.append(float(data[feature]))
            except ValueError:
                pass

    if vals:
        from validation.psi_calculator import compute_psi, psi_severity
        from drift_detection.baseline_manager import load_baseline

        baseline = load_baseline(feature) or vals
        psi = compute_psi(baseline, vals)
        severity = psi_severity(psi)
        color_map = {"ok": "🟢", "warn": "🟡", "alert": "🔴"}
        cols[i % 3].metric(
            feature.replace("_", " ").title(),
            f"PSI: {psi:.4f}",
            delta=f"{color_map.get(severity, '⚪')} {severity.upper()}",
        )

st.divider()

# ── Historical drift from PostgreSQL ─────────────────────────────────────────
st.subheader("Historical PSI Trend (24h)")
df = fetch_drift_metrics(hours=24)

if not df.empty:
    psi_df = df[df["metric_type"] == "psi_score"]
    if not psi_df.empty:
        fig = px.line(
            psi_df,
            x="recorded_at",
            y="metric_value",
            color="feature_name",
            title="PSI Over Time",
            labels={"metric_value": "PSI Score", "recorded_at": "Time"},
        )
        fig.add_hline(y=0.1, line_dash="dash", line_color="orange", annotation_text="Warn (0.1)")
        fig.add_hline(y=0.2, line_dash="dash", line_color="red", annotation_text="Alert (0.2)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No historical PSI data yet. Run the drift_monitoring Airflow DAG to generate metrics.")
else:
    st.info("No historical drift data in PostgreSQL yet.")

# ── Distribution comparison ───────────────────────────────────────────────────
st.subheader("Feature Distribution Comparison")
selected_feature = st.selectbox("Select feature", FEATURES)

current_vals = []
for uid in client.srandmember("index:active_users", 300):
    data = client.hgetall(f"features:{uid}")
    if selected_feature in data:
        try:
            current_vals.append(float(data[selected_feature]))
        except ValueError:
            pass

from drift_detection.baseline_manager import load_baseline
baseline_vals = load_baseline(selected_feature)

if current_vals:
    fig2 = px.histogram(title=f"{selected_feature} Distribution")

    if baseline_vals:
        fig2.add_trace(px.histogram(
            x=baseline_vals[:300], nbins=20, labels={"x": selected_feature}
        ).data[0])
        fig2.data[-1].name = "Baseline"
        fig2.data[-1].marker.color = "rgba(68, 114, 196, 0.6)"

    fig2.add_trace(px.histogram(
        x=current_vals, nbins=20
    ).data[0])
    fig2.data[-1].name = "Current"
    fig2.data[-1].marker.color = "rgba(237, 125, 49, 0.6)"

    fig2.update_layout(barmode="overlay")
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No current data in Redis for this feature.")
