import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Feature Freshness", layout="wide")
st.title("📅 Feature Freshness")
st.caption("Distribution of feature age in the online store (Redis)")

from monitoring.data_fetcher import fetch_feature_freshness, fetch_active_user_count

sample_n = st.sidebar.slider("Sample size", 50, 500, 200, 50)

with st.spinner("Loading freshness data..."):
    df = fetch_feature_freshness(sample_n=sample_n)

if df.empty:
    st.warning("No feature data found. Is the streaming pipeline running?")
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Median Age (s)", f"{df['freshness_seconds'].median():.1f}")
col2.metric("P95 Age (s)", f"{df['freshness_seconds'].quantile(0.95):.1f}")
sla_violations = (df["freshness_seconds"] > 300).sum()
col3.metric("SLA Violations (>300s)", sla_violations, delta=f"{sla_violations/len(df)*100:.1f}%", delta_color="inverse")

fig = px.histogram(
    df,
    x="freshness_seconds",
    nbins=30,
    title="Feature Age Distribution",
    labels={"freshness_seconds": "Age (seconds)"},
    color_discrete_sequence=["#4C8BF5"],
)
fig.add_vline(x=300, line_dash="dash", line_color="red", annotation_text="TTL (300s)")
fig.add_vline(x=60, line_dash="dash", line_color="orange", annotation_text="SLA (60s)")
st.plotly_chart(fig, use_container_width=True)

st.dataframe(
    df.sort_values("freshness_seconds", ascending=False).head(20),
    use_container_width=True,
)
