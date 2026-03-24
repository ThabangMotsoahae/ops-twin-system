import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# API Configuration
API_URL = "http://localhost:8000"

st.set_page_config(
    page_title="OpsTwin Dashboard",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("🛰️ OpsTwin - Operational Digital Twin")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Sidebar
with st.sidebar:
    st.header("⚙️ Controls")
    
    # Simulation steps slider
    sim_steps = st.slider(
        "Simulation Steps",
        min_value=1,
        max_value=10,
        value=5,
        help="Number of future time steps to simulate"
    )
    
    # Risk threshold slider
    risk_threshold = st.slider(
        "Risk Threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.7,
        step=0.05,
        help="Assets above this threshold are considered high-risk"
    )
    
    st.divider()
    
    # Refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# Function to fetch data from API with caching
@st.cache_data(ttl=30)  # Cache for 30 seconds
def fetch_states():
    try:
        response = requests.get(f"{API_URL}/states", timeout=5)
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        return None
    except:
        return None

@st.cache_data(ttl=30)
def fetch_metrics():
    try:
        response = requests.get(f"{API_URL}/metrics", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

@st.cache_data(ttl=30)
def fetch_simulation(steps):
    try:
        response = requests.get(f"{API_URL}/simulate/{steps}", timeout=5)
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        return None
    except:
        return None

@st.cache_data(ttl=30)
def fetch_high_risk(threshold):
    try:
        response = requests.get(f"{API_URL}/high-risk/{threshold}", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

# Fetch data
states_df = fetch_states()
metrics = fetch_metrics()
simulation_df = fetch_simulation(sim_steps)
high_risk_data = fetch_high_risk(risk_threshold)

# Check API connection
if states_df is None:
    st.error("❌ Cannot connect to OpsTwin API. Make sure it's running: uvicorn api.main:app --reload")
    st.stop()

# ==================== KPI ROW ====================
st.subheader("📊 Key Performance Indicators")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Assets",
        metrics["total_assets"] if metrics else "N/A",
        help="Total number of assets being monitored"
    )

with col2:
    if metrics:
        avg_risk = metrics["average_risk"]
        st.metric(
            "Average Risk Score",
            f"{avg_risk:.2f}",
            delta=None,
            help="Average risk across all assets (0=Healthy, 1=Critical)"
        )

with col3:
    if metrics:
        high_risk_count = high_risk_data["count"] if high_risk_data else 0
        st.metric(
            "High-Risk Assets",
            high_risk_count,
            delta=None,
            help=f"Assets with risk score > {risk_threshold}"
        )

with col4:
    if metrics:
        max_risk = metrics["max_risk"]
        st.metric(
            "Max Risk Score",
            f"{max_risk:.2f}",
            delta=None,
            help="Highest risk score across all assets"
        )

st.divider()

# ==================== MAIN CONTENT ====================
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📈 Asset State Distribution")
    
    if metrics and "state_distribution" in metrics:
        state_df = pd.DataFrame(
            metrics["state_distribution"].items(),
            columns=["State", "Count"]
        )
        
        # Color mapping
        color_map = {
            "HEALTHY": "#2ecc71",
            "WARNING": "#f39c12",
            "CRITICAL": "#e74c3c",
            "FAILURE": "#95a5a6"
        }
        
        fig = px.pie(
            state_df,
            values="Count",
            names="State",
            title="Asset States",
            color="State",
            color_discrete_map=color_map,
            hole=0.3
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("🏭 Assets by Type")
    
    if metrics and "assets_by_type" in metrics:
        type_df = pd.DataFrame(
            metrics["assets_by_type"].items(),
            columns=["Asset Type", "Count"]
        )
        
        fig = px.bar(
            type_df,
            x="Asset Type",
            y="Count",
            title="Asset Distribution by Type",
            color="Asset Type",
            text="Count"
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ==================== ASSET STATUS TABLE ====================
st.subheader("📋 Asset Status Dashboard")

if states_df is not None:
    # Add color coding for states
    def color_state(val):
        colors = {
            "HEALTHY": "background-color: #2ecc71; color: white",
            "WARNING": "background-color: #f39c12; color: white",
            "CRITICAL": "background-color: #e74c3c; color: white",
            "FAILURE": "background-color: #95a5a6; color: white"
        }
        return colors.get(val, "")
    
    # Format dataframe for display
    display_df = states_df.copy()
    display_df["risk_score"] = display_df["risk_score"].round(3)
    
    # Apply styling
    styled_df = display_df.style.applymap(
        color_state, subset=["state"]
    )
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "asset_id": "Asset ID",
            "asset_type": "Type",
            "location": "Location",
            "state": "State",
            "failure_count": "Failures",
            "downtime_hours": "Downtime (hrs)",
            "risk_score": st.column_config.NumberColumn("Risk Score", format="%.3f")
        }
    )

st.divider()

# ==================== SIMULATION TRENDS ====================
st.subheader("🔮 Risk Simulation Over Time")

if simulation_df is not None:
    # Create risk trend chart
    risk_trend = simulation_df.groupby(["step", "asset_id"])["risk_score"].mean().reset_index()
    
    fig = px.line(
        risk_trend,
        x="step",
        y="risk_score",
        color="asset_id",
        title="Risk Score Evolution by Asset",
        labels={"step": "Time Step", "risk_score": "Risk Score"},
        markers=True
    )
    
    fig.update_layout(
        xaxis=dict(tickmode='linear', tick0=1, dtick=1),
        yaxis=dict(range=[0, 1.1])
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Heatmap view
    st.subheader("📊 Risk Heatmap")
    
    # Create pivot table for heatmap
    heatmap_data = simulation_df.pivot_table(
        index="asset_id",
        columns="step",
        values="risk_score",
        fill_value=0
    )
    
    fig = px.imshow(
        heatmap_data,
        text_auto=True,
        aspect="auto",
        title="Risk Score Heatmap (Asset vs Time Step)",
        labels={"x": "Time Step", "y": "Asset ID", "color": "Risk Score"},
        color_continuous_scale="RdYlGn_r",
        zmin=0,
        zmax=1
    )
    
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ==================== HIGH-RISK ALERTS ====================
st.subheader("⚠️ High-Risk Assets Alert")

if high_risk_data and high_risk_data["count"] > 0:
    st.warning(f"🚨 **{high_risk_data['count']} assets** are above the risk threshold ({risk_threshold})")
    
    high_risk_df = pd.DataFrame(high_risk_data["assets"])
    
    for _, asset in high_risk_df.iterrows():
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            st.write(f"**{asset['asset_id']}**")
        with col2:
            st.write(f"State: {asset['state']}")
        with col3:
            risk_score = asset['risk_score']
            if risk_score > 0.8:
                st.error(f"Risk: {risk_score:.2f}")
            else:
                st.warning(f"Risk: {risk_score:.2f}")
else:
    st.success("✅ No assets currently above the risk threshold")

st.divider()

# ==================== FOOTER ====================
st.caption("🛰️ OpsTwin - Operational Digital Twin System | Built with FastAPI, Streamlit & Plotly")
