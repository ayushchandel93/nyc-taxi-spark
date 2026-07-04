# dashboard/app.py

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide"
)

GOLD_PATH = Path("data/gold")

# ── Data Loader ───────────────────────────────────────────────────────────────

@st.cache_data
def load_table(table: str) -> pd.DataFrame:
    return pd.read_parquet(GOLD_PATH / table)

# ── Load All Tables ───────────────────────────────────────────────────────────

daily    = load_table("daily_metrics")
hourly   = load_table("hourly_metrics")
zone     = load_table("zone_metrics")
payment  = load_table("payment_metrics")
weekday  = load_table("weekday_metrics")

daily["pickup_date"] = pd.to_datetime(daily["pickup_date"])
daily = daily.sort_values("pickup_date")

# ── Header ────────────────────────────────────────────────────────────────────

st.title("🚕 NYC Taxi Analytics Dashboard")
st.caption("Jan–Feb 2024 · 5.4M trips · Built with PySpark + Streamlit")

# ── Filters ───────────────────────────────────────────────────────────────────

months = sorted(daily["pickup_month"].unique())
selected_month = st.sidebar.selectbox(
    "Select Month", 
    ["All"] + list(months)
)

if selected_month != "All":
    daily   = daily[daily["pickup_month"]   == selected_month]
    hourly  = hourly[hourly["pickup_month"] == selected_month]
    zone    = zone[zone["pickup_month"]     == selected_month]
    payment = payment[payment["pickup_month"] == selected_month]
    weekday = weekday[weekday["pickup_month"] == selected_month]

# ── KPI Cards ─────────────────────────────────────────────────────────────────

st.subheader("Key Metrics")
k1, k2, k3, k4, k5 = st.columns(5)

k1.metric("Total Trips",    f"{daily['total_trips'].sum():,.0f}")
k2.metric("Total Revenue",  f"${daily['total_revenue'].sum():,.0f}")
k3.metric("Avg Fare",       f"${daily['avg_fare'].mean():,.2f}")
k4.metric("Avg Distance",   f"{daily['avg_distance'].mean():,.2f} mi")
k5.metric("Avg Tip",        f"{daily['avg_tip_pct'].mean():,.1f}%")

st.divider()

# ── Row 1: Daily Revenue + Hourly Pattern ─────────────────────────────────────

col1, col2 = st.columns(2)

with col1:
    st.subheader("Daily Revenue Trend")
    fig = px.line(
        daily,
        x="pickup_date",
        y="total_revenue",
        color="is_weekend",
        color_discrete_map={True: "#f97316", False: "#3b82f6"},
        labels={
            "pickup_date":    "Date",
            "total_revenue":  "Revenue ($)",
            "is_weekend":     "Weekend"
        }
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend_title="Weekend",
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Trip Volume by Hour")
    hourly_avg = hourly.groupby(["pickup_hour", "time_of_day"])[
        "total_trips"
    ].mean().reset_index()

    color_map = {
        "Morning":    "#3b82f6",
        "Afternoon":  "#f59e0b",
        "Evening":    "#f97316",
        "Night":      "#8b5cf6",
        "Late Night": "#6b7280"
    }

    fig = px.bar(
        hourly_avg.sort_values("pickup_hour"),
        x="pickup_hour",
        y="total_trips",
        color="time_of_day",
        color_discrete_map=color_map,
        labels={
            "pickup_hour":  "Hour of Day",
            "total_trips":  "Avg Trips",
            "time_of_day":  "Time of Day"
        }
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Row 2: Weekday Pattern + Payment Breakdown ────────────────────────────────

col3, col4 = st.columns(2)

with col3:
    st.subheader("Trips by Day of Week")
    weekday_avg = weekday.groupby(["day_name", "pickup_dayofweek", "is_weekend"])[
        "total_trips"
    ].sum().reset_index().sort_values("pickup_dayofweek")

    fig = px.bar(
        weekday_avg,
        x="day_name",
        y="total_trips",
        color="is_weekend",
        color_discrete_map={True: "#f97316", False: "#3b82f6"},
        labels={
            "day_name":    "Day",
            "total_trips": "Total Trips",
            "is_weekend":  "Weekend"
        }
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("Revenue by Payment Type")
    payment_labels = {1: "Credit Card", 2: "Cash", 3: "No Charge", 4: "Dispute"}
    payment["payment_label"] = payment["payment_type"].map(payment_labels).fillna("Unknown")

    payment_agg = payment.groupby("payment_label")["total_revenue"].sum().reset_index()

    fig = px.pie(
        payment_agg,
        names="payment_label",
        values="total_revenue",
        color_discrete_sequence=["#3b82f6", "#f97316", "#8b5cf6", "#10b981", "#6b7280"],
        hole=0.4
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Row 3: Top Zones + Daily Trips Table ─────────────────────────────────────

col5, col6 = st.columns(2)

with col5:
    st.subheader("Top 15 Pickup Zones by Revenue")
    top_zones = zone.groupby("PULocationID")["total_revenue"] \
                    .sum().reset_index() \
                    .sort_values("total_revenue", ascending=False) \
                    .head(15)

    fig = px.bar(
        top_zones,
        x="total_revenue",
        y="PULocationID",
        orientation="h",
        labels={
            "total_revenue": "Revenue ($)",
            "PULocationID":  "Zone ID"
        },
        color="total_revenue",
        color_continuous_scale="Blues"
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(type="category")
    )
    st.plotly_chart(fig, use_container_width=True)

with col6:
    st.subheader("Daily Summary Table")
    display_df = daily[["pickup_date", "total_trips", "total_revenue",
                         "avg_fare", "avg_distance", "avg_tip_pct", "is_weekend"]] \
                 .copy()
    display_df["pickup_date"]   = display_df["pickup_date"].dt.strftime("%Y-%m-%d")
    display_df["total_revenue"] = display_df["total_revenue"].apply(lambda x: f"${x:,.0f}")
    display_df["avg_fare"]      = display_df["avg_fare"].apply(lambda x: f"${x:,.2f}")
    display_df["total_trips"]   = display_df["total_trips"].apply(lambda x: f"{x:,.0f}")
    display_df.columns = ["Date", "Trips", "Revenue", "Avg Fare",
                          "Avg Distance", "Avg Tip %", "Weekend"]
    st.dataframe(display_df, use_container_width=True, height=400)

# ── Footer ────────────────────────────────────────────────────────────────────

st.divider()
st.caption("Data: NYC TLC Trip Record Data · Pipeline: PySpark medallion architecture (Bronze → Silver → Gold)")