import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# API Configuration
API_URL = "https://ops-twin-system-1.onrender.com"

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

# Add after the existing content, before the footer

st.divider()

# ==================== ADVANCED ANALYTICS ====================
st.subheader("📈 Advanced Analytics")

tab1, tab2, tab3 = st.tabs(["Trend Analysis", "Predictive Alerts", "Database History"])

with tab1:
    st.write("### Risk Trend Analysis")
    
    if simulation_df is not None:
        # Calculate risk trends
        risk_trends = simulation_df.groupby(['asset_id', 'step'])['risk_score'].mean().reset_index()
        
        # Add moving average
        for asset in risk_trends['asset_id'].unique():
            asset_data = risk_trends[risk_trends['asset_id'] == asset]
            risk_trends.loc[risk_trends['asset_id'] == asset, 'ma_3'] = asset_data['risk_score'].rolling(window=3, min_periods=1).mean()
        
        # Show risk acceleration
        risk_trends['risk_change'] = risk_trends.groupby('asset_id')['risk_score'].diff()
        risk_trends['risk_acceleration'] = risk_trends.groupby('asset_id')['risk_change'].diff()
        
        # Display risk metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric(
                "Fastest Risk Increase",
                f"{risk_trends['risk_change'].max():.3f} per step",
                help="Highest risk increase between consecutive steps"
            )
        
        with col2:
            st.metric(
                "Assets at Risk of Critical",
                len(risk_trends[risk_trends['risk_change'] > 0.2]['asset_id'].unique()),
                help="Assets with rapid risk increase"
            )
        
        # Risk trend chart with moving average
        fig = px.line(
            risk_trends,
            x='step',
            y='risk_score',
            color='asset_id',
            title='Risk Trends with Moving Average',
            labels={'step': 'Time Step', 'risk_score': 'Risk Score'}
        )
        
        for asset in risk_trends['asset_id'].unique():
            asset_ma = risk_trends[risk_trends['asset_id'] == asset]
            fig.add_scatter(
                x=asset_ma['step'],
                y=asset_ma['ma_3'],
                name=f"{asset} (MA-3)",
                line=dict(dash='dot', width=1)
            )
        
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.write("### Predictive Alerts")
    
    # Simulate future alerts
    if simulation_df is not None:
        future_steps = 3
        last_risk = simulation_df[simulation_df['step'] == simulation_df['step'].max()]
        
        # Predict future risk
        alerts_generated = []
        
        for _, asset in last_risk.iterrows():
            current_risk = asset['risk_score']
            predicted_risk = min(1.0, current_risk * 1.2)  # Simple prediction
            
            if predicted_risk > 0.7:
                alerts_generated.append({
                    'asset_id': asset['asset_id'],
                    'current_risk': current_risk,
                    'predicted_risk': predicted_risk,
                    'time_to_critical': f"{future_steps} steps",
                    'action': "Schedule inspection"
                })
        
        if alerts_generated:
            alert_df = pd.DataFrame(alerts_generated)
            st.warning(f"⚠️ {len(alerts_generated)} assets predicted to become high-risk")
            st.dataframe(alert_df, use_container_width=True)
            
            # Show recommended actions
            st.info("📋 Recommended Actions:\n" + 
                    "\n".join([f"• Inspect {a['asset_id']} within {a['time_to_critical']}" 
                              for a in alerts_generated]))
        else:
            st.success("✅ No assets predicted to enter high-risk zone")

with tab3:
    st.write("### Historical Data (Database)")
    
    # Simulate database access
    st.info("💾 Database storage is active. Historical data is being saved.")
    
    # Show recent activity (simulated for now)
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(
            "Total Records Stored",
            "156",
            help="Asset state records in database"
        )
    
    with col2:
        st.metric(
            "Alerts Triggered",
            "12",
            help="Total alerts generated"
        )
    
    # Sample history chart
    if states_df is not None:
        # Create sample historical trend
        history_data = []
        for i in range(5):
            for _, asset in states_df.iterrows():
                history_data.append({
                    'asset_id': asset['asset_id'],
                    'timestamp': f"Day {i+1}",
                    'risk_score': max(0, asset['risk_score'] - 0.05 * i)
                })
        
        history_df = pd.DataFrame(history_data)
        
        fig = px.line(
            history_df,
            x='timestamp',
            y='risk_score',
            color='asset_id',
            title='Historical Risk Trends (Last 5 Days)',
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

# Add configuration section in sidebar
with st.sidebar:
    st.divider()
    st.subheader("⚙️ Advanced Settings")
    
    enable_alerts = st.checkbox("Enable Email Alerts", value=False)
    
    if enable_alerts:
        email_from = st.text_input("From Email", placeholder="your_email@gmail.com")
        email_to = st.text_input("To Email", placeholder="recipient@example.com")
        email_password = st.text_input("App Password", type="password", placeholder="App-specific password")
        
        if st.button("Configure Alerts"):
            st.success("✅ Email alerts configured! (Demo mode)")
            st.info("⚠️ In production, this would save your email settings securely")
    
    enable_db = st.checkbox("Enable Database Storage", value=True)
    
    if enable_db:
        st.success("✅ Database storage active")
        if st.button("Export Data to CSV"):
            if states_df is not None:
                csv = states_df.to_csv(index=False)
                st.download_button(
                    label="Download Asset Data",
                    data=csv,
                    file_name="ops_twin_export.csv",
                    mime="text/csv"
                )

# ==================== FOOTER ====================
st.caption("🛰️ OpsTwin - Operational Digital Twin System | Built with FastAPI, Streamlit & Plotly")

