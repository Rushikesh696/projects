import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import time
import random
from datetime import datetime, timedelta

st.set_page_config(page_title="PredictMaint Dashboard", layout="wide", page_icon="⚙️")
st.title("PredictMaint - Real-Time Predictive Maintenance Dashboard")

MACHINE_TYPES = ["Pump", "Compressor", "Motor", "Turbine", "Conveyor"]
LOCATIONS = ["Plant-A", "Plant-B", "Plant-C", "Plant-D"]

RISK_COLORS = {"LOW": "green", "MEDIUM": "orange", "HIGH": "red", "CRITICAL": "darkred"}


def simulate_machine_data(n=50):
    machines = []
    for i in range(1, n + 1):
        prob = random.uniform(0, 1)
        risk = "LOW" if prob < 0.25 else "MEDIUM" if prob < 0.50 else "HIGH" if prob < 0.75 else "CRITICAL"
        machines.append({
            "machine_id": f"M{i:03d}",
            "machine_type": random.choice(MACHINE_TYPES),
            "location": random.choice(LOCATIONS),
            "temperature": round(random.uniform(40, 130), 1),
            "vibration": round(random.uniform(0.1, 8.0), 2),
            "pressure": round(random.uniform(1.0, 40.0), 1),
            "rpm": round(random.uniform(100, 6000), 0),
            "voltage": round(random.uniform(200, 265), 1),
            "failure_probability": round(prob, 3),
            "risk_level": risk,
            "last_updated": datetime.now().strftime("%H:%M:%S"),
        })
    return pd.DataFrame(machines)


# ─── Sidebar ────────────────────────────────────────────────────────────────
st.sidebar.header("Filters")
selected_location = st.sidebar.selectbox("Location", ["All"] + LOCATIONS)
selected_type = st.sidebar.selectbox("Machine Type", ["All"] + MACHINE_TYPES)
risk_filter = st.sidebar.multiselect("Risk Level", ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
                                      default=["LOW", "MEDIUM", "HIGH", "CRITICAL"])
auto_refresh = st.sidebar.checkbox("Auto Refresh (5s)", value=False)

# ─── Load Data ──────────────────────────────────────────────────────────────
df = simulate_machine_data()
if selected_location != "All":
    df = df[df["location"] == selected_location]
if selected_type != "All":
    df = df[df["machine_type"] == selected_type]
df = df[df["risk_level"].isin(risk_filter)]

# ─── KPI Cards ──────────────────────────────────────────────────────────────
st.subheader("Fleet Overview")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Machines", len(df))
col2.metric("Critical", len(df[df["risk_level"] == "CRITICAL"]), delta=None)
col3.metric("High Risk",  len(df[df["risk_level"] == "HIGH"]))
col4.metric("Avg Failure Prob", f"{df['failure_probability'].mean():.1%}")

st.divider()

# ─── Risk Distribution ──────────────────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Risk Distribution")
    risk_counts = df["risk_level"].value_counts().reset_index()
    risk_counts.columns = ["Risk Level", "Count"]
    colors = [RISK_COLORS.get(r, "grey") for r in risk_counts["Risk Level"]]
    fig_pie = px.pie(risk_counts, names="Risk Level", values="Count",
                     color="Risk Level",
                     color_discrete_map=RISK_COLORS)
    st.plotly_chart(fig_pie, use_container_width=True)

with col_b:
    st.subheader("Failure Probability by Machine Type")
    fig_box = px.box(df, x="machine_type", y="failure_probability",
                     color="machine_type", points="all")
    st.plotly_chart(fig_box, use_container_width=True)

# ─── Machine Table ──────────────────────────────────────────────────────────
st.subheader("Machine Status Table")

def color_risk(val):
    colors_map = {"LOW": "background-color: #d4edda", "MEDIUM": "background-color: #fff3cd",
                  "HIGH": "background-color: #f8d7da", "CRITICAL": "background-color: #721c24; color: white"}
    return colors_map.get(val, "")

display_df = df[["machine_id", "machine_type", "location", "temperature",
                  "vibration", "pressure", "rpm", "failure_probability", "risk_level", "last_updated"]]

st.dataframe(
    display_df.style.applymap(color_risk, subset=["risk_level"]),
    use_container_width=True,
    height=400
)

# ─── Sensor Trend (Simulated) ───────────────────────────────────────────────
st.subheader("Sensor Trend (Live Simulation)")
selected_machine = st.selectbox("Select Machine", df["machine_id"].tolist())

times = [datetime.now() - timedelta(minutes=30 - i) for i in range(30)]
temp_trend  = [round(random.uniform(60, 110), 1) for _ in range(30)]
vib_trend   = [round(random.uniform(0.5, 7.0), 2) for _ in range(30)]

fig_trend = go.Figure()
fig_trend.add_trace(go.Scatter(x=times, y=temp_trend,  name="Temperature (°C)", line=dict(color="red")))
fig_trend.add_trace(go.Scatter(x=times, y=vib_trend,   name="Vibration (mm/s)", line=dict(color="blue"), yaxis="y2"))
fig_trend.update_layout(
    title=f"Sensor Trends - {selected_machine} (Last 30 mins)",
    yaxis=dict(title="Temperature (°C)"),
    yaxis2=dict(title="Vibration (mm/s)", overlaying="y", side="right"),
    legend=dict(x=0, y=1.1, orientation="h")
)
st.plotly_chart(fig_trend, use_container_width=True)

# ─── Auto Refresh ───────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(5)
    st.rerun()
