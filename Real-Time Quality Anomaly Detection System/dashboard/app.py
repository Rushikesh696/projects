import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

st.set_page_config(
    page_title="Serum QA Anomaly Detection",
    layout="wide",
    initial_sidebar_state="expanded",
)

REFRESH_INTERVAL = 30  # seconds


# ─── DB helpers ───────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# ─── Data loaders (cached 30s) ────────────────────────────────────────────────

@st.cache_data(ttl=REFRESH_INTERVAL)
def load_kpis() -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), SUM(is_anomaly) FROM gold.qa_features")
            total, anomalies = cur.fetchone()

            cur.execute("SELECT COUNT(*) FROM public.alerts WHERE status = 'open'")
            open_alerts = cur.fetchone()[0]

        anomalies = int(anomalies or 0)
        total = int(total or 0)
        return {
            "total_events": total,
            "total_anomalies": anomalies,
            "open_alerts": int(open_alerts or 0),
            "anomaly_rate_pct": round(anomalies / total * 100, 3) if total else 0.0,
        }
    finally:
        conn.close()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_7day_trend() -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql(
            """
            SELECT
                DATE(timestamp)    AS date,
                COUNT(*)           AS total_events,
                SUM(is_anomaly)    AS anomalies
            FROM gold.qa_features
            WHERE timestamp >= (
                SELECT MAX(timestamp) FROM gold.qa_features
            ) - INTERVAL '7 days'
            GROUP BY DATE(timestamp)
            ORDER BY date
            """,
            conn,
        )
        df["date"] = pd.to_datetime(df["date"])
        return df.set_index("date")
    finally:
        conn.close()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_system_breakdown() -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql(
            """
            SELECT
                system,
                COUNT(*)                                    AS total_events,
                SUM(is_anomaly)                             AS anomalies,
                ROUND(AVG(is_anomaly::float)::numeric * 100, 2)     AS anomaly_rate_pct
            FROM gold.qa_features
            GROUP BY system
            ORDER BY anomalies DESC
            """,
            conn,
        )
        return df
    finally:
        conn.close()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_severity_breakdown() -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql(
            """
            SELECT severity, COUNT(*) AS count
            FROM gold.qa_features
            WHERE is_anomaly = 1
            GROUP BY severity
            ORDER BY count DESC
            """,
            conn,
        )
        return df.set_index("severity")
    finally:
        conn.close()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_recent_alerts(limit: int = 100) -> pd.DataFrame:
    conn = get_conn()
    try:
        df = pd.read_sql(
            "SELECT id, timestamp, event_id, event_type, system, product, "
            "severity, anomaly_score, alert_reason, status "
            "FROM public.alerts ORDER BY timestamp DESC LIMIT %s",
            conn,
            params=(limit,),
        )
        return df
    finally:
        conn.close()


# ─── Sidebar ──────────────────────────────────────────────────────────────────

st.sidebar.title("Serum Institute QA")
st.sidebar.markdown("**Real-Time Anomaly Detection**")
st.sidebar.markdown("---")

auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=True)
alert_status_filter = st.sidebar.selectbox("Alert status filter", ["all", "open", "closed"])
alert_limit = st.sidebar.slider("Max alerts to display", 10, 500, 50)

st.sidebar.markdown("---")
st.sidebar.markdown(f"Last refresh: `{datetime.now().strftime('%H:%M:%S')}`")


# ─── Header ───────────────────────────────────────────────────────────────────

st.title("Serum QA — Anomaly Detection Dashboard")
st.markdown("Monitoring `gold.qa_features` | Models: Isolation Forest + Autoencoder")
st.markdown("---")


# ─── KPI row ──────────────────────────────────────────────────────────────────

kpis = load_kpis()
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Events", f"{kpis['total_events']:,}")
col2.metric("Total Anomalies", f"{kpis['total_anomalies']:,}")
col3.metric("Open Alerts", kpis["open_alerts"])
col4.metric("Overall Anomaly Rate", f"{kpis['anomaly_rate_pct']}%")

st.markdown("---")


# ─── Trend + System breakdown ─────────────────────────────────────────────────

left, right = st.columns(2)

with left:
    st.subheader("7-Day Anomaly Trend")
    trend_df = load_7day_trend()
    if trend_df.empty:
        st.info("No data in the last 7 days.")
    else:
        st.line_chart(trend_df[["anomalies", "total_events"]])

with right:
    st.subheader("Anomalies by System")
    sys_df = load_system_breakdown()
    if sys_df.empty:
        st.info("No system data available.")
    else:
        st.bar_chart(sys_df.set_index("system")["anomalies"])

st.markdown("---")


# ─── Severity breakdown ───────────────────────────────────────────────────────

st.subheader("Anomaly Severity Breakdown")
sev_df = load_severity_breakdown()
if sev_df.empty:
    st.info("No anomaly severity data.")
else:
    st.bar_chart(sev_df)

st.markdown("---")


# ─── Alert feed ───────────────────────────────────────────────────────────────

st.subheader(f"Recent Alerts (last {alert_limit})")
alerts_df = load_recent_alerts(limit=alert_limit)

if alert_status_filter != "all" and not alerts_df.empty:
    alerts_df = alerts_df[alerts_df["status"] == alert_status_filter]

if alerts_df.empty:
    st.success("No alerts matching the current filter.")
else:
    st.dataframe(
        alerts_df.style.applymap(
            lambda v: "background-color: #ffcccc" if v == "open" else "",
            subset=["status"],
        ),
        use_container_width=True,
    )

st.markdown("---")


# ─── System detail drill-down ─────────────────────────────────────────────────

st.subheader("System Detail")
systems = load_system_breakdown()
if not systems.empty:
    selected_system = st.selectbox("Select system", systems["system"].tolist())
    system_row = systems[systems["system"] == selected_system].iloc[0]
    d1, d2, d3 = st.columns(3)
    d1.metric("Total Events", f"{int(system_row['total_events']):,}")
    d2.metric("Anomalies", f"{int(system_row['anomalies']):,}")
    d3.metric("Anomaly Rate", f"{system_row['anomaly_rate_pct']}%")


# ─── Auto-refresh ─────────────────────────────────────────────────────────────

if auto_refresh:
    time.sleep(REFRESH_INTERVAL)
    st.rerun()
