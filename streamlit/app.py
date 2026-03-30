"""AMTAB Bari: On-Time Performance Dashboard"""

import os

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET = os.environ.get("BQ_DATASET", "amtab_transit")

st.set_page_config(page_title="AMTAB OTP Dashboard", layout="wide")
st.title("AMTAB Bari — On-Time Performance")


@st.cache_data(ttl=3600)
def query_otp_data():
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"""
        SELECT
            m.dt,
            m.route_id,
            r.route_short_name,
            r.route_long_name,
            m.total_observations,
            m.on_time_count,
            m.otp_pct
        FROM `{PROJECT_ID}.{BQ_DATASET}.mart_otp_summary` m
        LEFT JOIN `{PROJECT_ID}.{BQ_DATASET}.routes` r
            ON m.route_id = r.route_id
        ORDER BY m.dt
    """
    return client.query(sql).to_dataframe()


df = query_otp_data()

# route label format: "01 — Bari Centrale - Santo Spirito"
df["route_label"] = df.apply(
    lambda r: (
        f"{r['route_short_name']} — {r['route_long_name']}"
        if r["route_long_name"]
        else r["route_id"]
    ),
    axis=1,
)

# Filters

col_date, col_route = st.columns(2)
with col_date:
    date_range = st.date_input(
        "Date range",
        value=(df["dt"].min(), df["dt"].max()),
        min_value=df["dt"].min(),
        max_value=df["dt"].max(),
    )
with col_route:
    route_options = sorted(df["route_label"].unique())
    selected_route = st.selectbox("Route", ["All"] + route_options)

if len(date_range) == 2:
    mask = (df["dt"] >= date_range[0]) & (df["dt"] <= date_range[1])
elif len(date_range) == 1:
    mask = df["dt"] >= date_range[0]
else:
    mask = df["dt"].notna()

filtered = df[mask]

# KPI cards

total_obs = filtered["total_observations"].sum()
total_on_time = filtered["on_time_count"].sum()
overall_otp = round(total_on_time / total_obs * 100, 1) if total_obs > 0 else 0
num_routes = filtered["route_id"].nunique()
num_days = filtered["dt"].nunique()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Overall OTP", f"{overall_otp}%")
k2.metric("Observations", f"{total_obs:,}")
k3.metric("Routes", num_routes)
k4.metric("Days", num_days)

# Tile 1: OTP trend and observation volume

st.subheader("Daily On-Time Performance")

if selected_route != "All":
    trend_df = filtered[filtered["route_label"] == selected_route].copy()
else:
    trend_df = filtered.groupby("dt", as_index=False).agg(
        {"on_time_count": "sum", "total_observations": "sum"}
    )
    trend_df["otp_pct"] = round(
        trend_df["on_time_count"] / trend_df["total_observations"] * 100, 2
    )

fig_trend = make_subplots(specs=[[{"secondary_y": True}]])

fig_trend.add_trace(
    go.Bar(
        x=trend_df["dt"],
        y=trend_df["total_observations"],
        name="Observations",
        marker_color="#f59e0b",
        opacity=0.4,
    ),
    secondary_y=False,
)

fig_trend.add_trace(
    go.Scatter(
        x=trend_df["dt"],
        y=trend_df["otp_pct"],
        name="OTP %",
        mode="lines",
        line=dict(color="#2563eb", width=2.5),
    ),
    secondary_y=True,
)

fig_trend.update_layout(
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(t=30),
    height=400,
)
fig_trend.update_yaxes(title_text="Observations", secondary_y=False)
fig_trend.update_yaxes(title_text="OTP %", range=[0, 100], secondary_y=True)

st.plotly_chart(fig_trend, use_container_width=True)

# Tile 2: Worst routes by OTP

st.subheader("Worst Routes by OTP")

min_obs = st.slider(
    "Minimum observations",
    min_value=0,
    max_value=5000,
    value=100,
    step=100,
    help="Exclude routes with too few observations to be statistically meaningful",
)

worst = filtered.groupby(["route_id", "route_label"], as_index=False).agg(
    {"on_time_count": "sum", "total_observations": "sum"}
)
worst["otp_pct"] = round(worst["on_time_count"] / worst["total_observations"] * 100, 1)
worst = worst[worst["total_observations"] >= min_obs]
worst = worst.nsmallest(10, "otp_pct").sort_values("otp_pct")

fig_worst = go.Figure()
fig_worst.add_trace(
    go.Bar(
        x=worst["otp_pct"],
        y=worst["route_label"],
        orientation="h",
        marker_color="#ef4444",
        text=[
            f"{otp}% ({obs:,} obs)"
            for otp, obs in zip(worst["otp_pct"], worst["total_observations"])
        ],
        textposition="outside",
        textfont=dict(size=12),
    )
)

bar_count = len(worst)
fig_worst.update_layout(
    xaxis=dict(title="OTP %", range=[0, 110]),
    yaxis=dict(categoryorder="total ascending", automargin=True),
    height=max(300, bar_count * 45 + 80),
    margin=dict(t=10, r=80),
)

st.plotly_chart(fig_worst, use_container_width=True)
