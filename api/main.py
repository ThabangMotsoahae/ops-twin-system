from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import pandas as pd
import sys
from pathlib import Path
from typing import Optional, List
import uuid
from datetime import datetime
import threading
import time
import random

# Add parent directory to path so we can import modules
sys.path.append(str(Path(__file__).parent.parent))

from state_engine.state_manager import assign_states
from state_engine.transitions import simulate_transitions
from risk_model.risk_engine import compute_risk
from risk_model.alert_system import alert_system
from risk_model.database_manager import db_manager

# ==================== CUSTOM STYLED APP ====================

app = FastAPI(
    title="OpsTwin API",
    description="""
    ## 🛰️ Operational Digital Twin for Risk & Reliability
    
    **OpsTwin** is a real-time operational intelligence system that monitors assets, 
    predicts future states, and provides actionable risk insights.
    
    ### Features:
    - 📡 **Real-time sensor simulation** (temperature, vibration, error codes)
    - 🧠 **AI-powered risk scoring** (0-1 scale)
    - 🔔 **Automatic alerts** for high-risk conditions
    - 📊 **Historical data tracking** with SQLite
    - 🚀 **REST API** with 14 endpoints
    
    ### Quick Links:
    - [GitHub Repository](https://github.com/ThabangMotsoahae/ops-twin-system)
    - [Live Demo](https://ops-twin-system-1.onrender.com)
    """,
    version="2.0.0",
    contact={
        "name": "Thabang Motsoahae",
        "url": "https://github.com/ThabangMotsoahae",
        "email": "thabangmotsoahae@axulo-inc.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "Core",
            "description": "Asset state and risk management endpoints"
        },
        {
            "name": "Real-Time",
            "description": "Live sensor data ingestion and monitoring"
        },
        {
            "name": "Alerts",
            "description": "Alert configuration and history"
        },
        {
            "name": "Database",
            "description": "Historical data storage and retrieval"
        }
    ]
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data path
DATA_PATH = "data/assets.csv"

# ==================== RESPONSE MODELS ====================

class AssetState(BaseModel):
    asset_id: str
    asset_type: str
    location: str
    state: str
    failure_count: int
    downtime_hours: int
    risk_score: float

class RiskResponse(BaseModel):
    asset_id: str
    state: str
    risk_score: float
    step: int

class SensorData(BaseModel):
    asset_id: str
    timestamp: Optional[str] = None
    temperature: Optional[float] = None
    vibration: Optional[float] = None
    pressure: Optional[float] = None
    engine_hours: Optional[int] = None
    fuel_level: Optional[float] = None
    error_code: Optional[str] = None
    custom_data: Optional[dict] = None

class BatchIngest(BaseModel):
    data: List[SensorData]
    batch_id: Optional[str] = None

# ==================== CUSTOM SWAGGER UI ====================

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return HTMLResponse(content="""
    <!DOCTYPE html>
    <html>
    <head>
        <title>OpsTwin API Documentation</title>
        <style>
            /* Custom styling for Swagger UI */
            .swagger-ui .topbar {
                background-color: #1a1a2e;
                padding: 10px 0;
            }
            .swagger-ui .topbar .download-url-wrapper .select-label {
                color: #fff;
            }
            .swagger-ui .info .title {
                color: #1a1a2e;
                font-size: 36px;
            }
            .swagger-ui .info .title small {
                background-color: #4a5568;
                color: white;
            }
            .swagger-ui .btn.authorize {
                border-color: #48bb78;
                color: #48bb78;
            }
            .swagger-ui .btn.authorize svg {
                fill: #48bb78;
            }
            .swagger-ui .opblock-tag {
                background-color: #f7fafc;
                border-left: 4px solid #4299e1;
            }
            .swagger-ui .opblock.opblock-get .opblock-summary-method {
                background-color: #48bb78;
            }
            .swagger-ui .opblock.opblock-post .opblock-summary-method {
                background-color: #4299e1;
            }
            .swagger-ui .response-col_status {
                font-weight: bold;
            }
            /* Custom header */
            .custom-header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 20px;
                text-align: center;
                margin-bottom: 20px;
                border-radius: 5px;
            }
            .custom-header h1 {
                margin: 0;
                font-size: 28px;
            }
            .custom-header p {
                margin: 10px 0 0;
                opacity: 0.9;
            }
            .badge {
                display: inline-block;
                background-color: #48bb78;
                color: white;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 12px;
                margin-left: 10px;
            }
            .stats-bar {
                display: flex;
                justify-content: center;
                gap: 20px;
                margin-top: 15px;
                flex-wrap: wrap;
            }
            .stat-item {
                background: rgba(255,255,255,0.2);
                padding: 5px 12px;
                border-radius: 20px;
                font-size: 12px;
            }
        </style>
        <link rel="icon" type="image/x-icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🛰️</text></svg>">
    </head>
    <body>
        <div class="custom-header">
            <h1>🛰️ OpsTwin API <span class="badge">v2.0.0</span></h1>
            <p>Operational Digital Twin for Risk & Reliability | AI-Powered Asset Intelligence</p>
            <div class="stats-bar">
                <span class="stat-item">📡 6 Active Assets</span>
                <span class="stat-item">⚡ Real-Time Updates (3s)</span>
                <span class="stat-item">🎯 14 API Endpoints</span>
                <span class="stat-item">🧠 AI Risk Scoring</span>
            </div>
            <p style="font-size: 14px; margin-top: 15px;">
                🔗 <a href="https://ops-twin-system-1.onrender.com" style="color: white;">Live API</a> | 
                📚 <a href="https://github.com/ThabangMotsoahae/ops-twin-system" style="color: white;">GitHub</a> |
                📊 <a href="/realtime/status" style="color: white;">System Status</a>
            </p>
        </div>
        <div id="swagger-ui"></div>
        <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
        <script>
            window.onload = function() {
                const ui = SwaggerUIBundle({
                    url: "/openapi.json",
                    dom_id: '#swagger-ui',
                    presets: [
                        SwaggerUIBundle.presets.apis,
                        SwaggerUIStandalonePreset
                    ],
                    layout: "BaseLayout",
                    deepLinking: true,
                    displayOperationId: false,
                    filter: true,
                    tryItOutEnabled: true,
                    persistAuthorization: true,
                });
                window.ui = ui;
            };
        </script>
    </body>
    </html>
    """)

# ==================== REAL-TIME DATA STORAGE ====================

realtime_asset_data = {}
simulation_running = False
simulation_thread = None

def process_sensor_data(data: dict):
    """Process incoming sensor data and update risk multiplier"""
    asset_id = data.get('asset_id')
    if not asset_id:
        return False
    
    # Calculate risk multiplier based on sensor readings
    risk_multiplier = 0
    
    # Temperature factor (high temp = higher risk)
    temp = data.get('temperature')
    if temp is not None:
        if temp > 70:
            risk_multiplier += 0.3
        elif temp > 50:
            risk_multiplier += 0.1
    
    # Vibration factor
    vibration = data.get('vibration')
    if vibration is not None:
        if vibration > 8:
            risk_multiplier += 0.4
        elif vibration > 5:
            risk_multiplier += 0.2
    
    # Error code factor
    error_code = data.get('error_code')
    if error_code and error_code != 'None' and error_code != 'null':
        risk_multiplier += 0.5
    
    # Fuel level factor
    fuel = data.get('fuel_level')
    if fuel is not None:
        if fuel < 10:
            risk_multiplier += 0.3
    
    # Add risk multiplier to data
    data['risk_multiplier'] = min(1.0, risk_multiplier)
    data['processed_at'] = datetime.now().isoformat()
    
    # Store in global dict
    realtime_asset_data[asset_id] = data
    
    # Trigger alert if high risk
    if risk_multiplier > 0.7:
        alert_msg = f"⚠️ High risk detected for {asset_id}: Temp={temp}°C, Vib={vibration}, Error={error_code}"
        print(alert_msg)
        
        # Save to database if available
        try:
            db_manager.save_alert("CRITICAL", asset_id, alert_msg)
        except:
            pass
    
    return True

def start_simulation(interval_seconds=3):
    """Start simulated real-time data generation"""
    global simulation_running, simulation_thread
    
    if simulation_running:
        return
    
    simulation_running = True
    
    def simulate():
        assets = ['TRK001', 'TRK002', 'DRL001', 'DRL002', 'LDR001', 'LDR002']
        
        while simulation_running:
            for asset_id in assets:
                # Generate random sensor data
                data = {
                    'asset_id': asset_id,
                    'timestamp': datetime.now().isoformat(),
                    'temperature': round(random.uniform(20, 85), 1),
                    'vibration': round(random.uniform(0, 10), 2),
                    'pressure': round(random.uniform(90, 110), 1),
                    'engine_hours': random.randint(0, 24),
                    'fuel_level': round(random.uniform(0, 100), 1),
                    'error_code': random.choice([None, None, None, 'E101', None, None, 'E202'])
                }
                
                # Process the data
                process_sensor_data(data)
            
            time.sleep(interval_seconds)
    
    simulation_thread = threading.Thread(target=simulate, daemon=True)
    simulation_thread.start()
    print(f"✅ Real-time simulation started (every {interval_seconds}s)")

def stop_simulation():
    """Stop the real-time simulation"""
    global simulation_running
    simulation_running = False
    print("✅ Real-time simulation stopped")

# Start simulation when API starts
@app.on_event("startup")
async def startup_event():
    """Start real-time data simulation on startup"""
    start_simulation(interval_seconds=3)

@app.on_event("shutdown")
async def shutdown_event():
    """Stop simulation on shutdown"""
    stop_simulation()

# ==================== CORE ENDPOINTS ====================

@app.get("/", tags=["Core"])
def root():
    return {
        "system": "OpsTwin",
        "version": "2.0.0",
        "description": "Operational Digital Twin for Risk & Reliability",
        "endpoints": [
            "/states",
            "/risk/{asset_id}",
            "/simulate/{steps}",
            "/high-risk/{threshold}",
            "/metrics",
            "/ingest/single",
            "/ingest/batch",
            "/realtime/latest",
            "/realtime/status",
            "/realtime/control",
            "/configure-alerts",
            "/alerts/history",
            "/assets/history/{asset_id}",
            "/save-state"
        ]
    }

@app.get("/states", response_model=list[AssetState], tags=["Core"])
def get_current_states():
    """
    Get current states for all assets
    
    Returns a list of all assets with their:
    - Current health state (HEALTHY/WARNING/CRITICAL/FAILURE)
    - Failure count and downtime hours
    - Risk score (0-1 scale)
    """
    try:
        df = assign_states(DATA_PATH)
        df_risk = compute_risk(df)
        
        result = []
        for _, row in df_risk.iterrows():
            result.append(AssetState(
                asset_id=row["asset_id"],
                asset_type=row["asset_type"],
                location=row["location"],
                state=row["state"],
                failure_count=row["failure_count"],
                downtime_hours=row["downtime_hours"],
                risk_score=row["risk_score"]
            ))
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/risk/{asset_id}", tags=["Core"])
def get_asset_risk(asset_id: str):
    """
    Get risk score for a specific asset
    
    Returns detailed information about a single asset including:
    - Current state
    - Risk score
    - Failure history
    - Downtime hours
    """
    try:
        df = assign_states(DATA_PATH)
        df_risk = compute_risk(df)
        
        asset = df_risk[df_risk["asset_id"] == asset_id]
        if asset.empty:
            raise HTTPException(status_code=404, detail=f"Asset {asset_id} not found")
        
        return {
            "asset_id": asset_id,
            "state": asset.iloc[0]["state"],
            "risk_score": asset.iloc[0]["risk_score"],
            "failure_count": int(asset.iloc[0]["failure_count"]),
            "downtime_hours": int(asset.iloc[0]["downtime_hours"])
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/simulate/{steps}", response_model=list[RiskResponse], tags=["Core"])
def simulate_future(steps: int):
    """
    Simulate state transitions for a number of steps
    
    Uses probabilistic Markov chain modeling to predict:
    - How asset states will evolve over time
    - Future risk scores
    - Potential failure points
    
    Steps: 1-20 (inclusive)
    """
    if steps < 1 or steps > 20:
        raise HTTPException(status_code=400, detail="Steps must be between 1 and 20")
    
    try:
        df = assign_states(DATA_PATH)
        df_history = simulate_transitions(df, steps=steps)
        df_risk = compute_risk(df_history)
        
        result = []
        for _, row in df_risk.iterrows():
            result.append(RiskResponse(
                asset_id=row["asset_id"],
                state=row["state"],
                risk_score=row["risk_score"],
                step=row["step"]
            ))
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/high-risk/{threshold}", tags=["Core"])
def get_high_risk_assets(threshold: float):
    """
    Get assets with risk score above threshold
    
    Use this to identify assets requiring immediate attention.
    Threshold should be between 0 and 1.
    """
    if threshold < 0 or threshold > 1:
        raise HTTPException(status_code=400, detail="Threshold must be between 0 and 1")
    
    try:
        df = assign_states(DATA_PATH)
        df_risk = compute_risk(df)
        
        high_risk = df_risk[df_risk["risk_score"] >= threshold]
        
        return {
            "threshold": threshold,
            "count": len(high_risk),
            "assets": [
                {
                    "asset_id": row["asset_id"],
                    "state": row["state"],
                    "risk_score": row["risk_score"]
                }
                for _, row in high_risk.iterrows()
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics", tags=["Core"])
def get_system_metrics():
    """
    Get system-wide metrics
    
    Returns:
    - Total assets monitored
    - State distribution (HEALTHY/WARNING/CRITICAL/FAILURE counts)
    - Average and maximum risk scores
    - Asset type breakdown
    """
    try:
        df = assign_states(DATA_PATH)
        df_risk = compute_risk(df)
        
        return {
            "total_assets": len(df_risk),
            "state_distribution": df_risk["state"].value_counts().to_dict(),
            "average_risk": float(df_risk["risk_score"].mean()),
            "max_risk": float(df_risk["risk_score"].max()),
            "assets_by_type": df_risk["asset_type"].value_counts().to_dict()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ALERT ENDPOINTS ====================

@app.post("/configure-alerts", tags=["Alerts"])
def configure_alerts(email_from: str, email_password: str, email_to: list[str]):
    """Configure email alert system"""
    try:
        alert_system.configure_email(email_from, email_password, email_to)
        return {"status": "success", "message": "Email alerts configured"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/alerts/history", tags=["Alerts"])
def get_alert_history(limit: int = 50):
    """Get alert history"""
    try:
        df = db_manager.get_alert_history(limit)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/assets/history/{asset_id}", tags=["Database"])
def get_asset_history(asset_id: str, limit: int = 100):
    """Get historical data for an asset"""
    try:
        df = db_manager.get_history(asset_id, limit)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/save-state", tags=["Database"])
def save_current_state():
    """Save current asset states to database"""
    try:
        df = assign_states(DATA_PATH)
        df_risk = compute_risk(df)
        db_manager.save_asset_states(df_risk)
        return {"status": "success", "message": "State saved to database"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== REAL-TIME DATA INGESTION ENDPOINTS ====================

@app.post("/ingest/single", tags=["Real-Time"])
def ingest_single_sensor_data(data: SensorData):
    """
    Ingest single sensor data point
    
    Send real-time sensor readings for a specific asset.
    The system will automatically calculate a risk multiplier
    based on temperature, vibration, error codes, and fuel level.
    """
    try:
        data_dict = data.dict()
        
        # Add timestamp if not provided
        if not data_dict.get('timestamp'):
            data_dict['timestamp'] = datetime.now().isoformat()
        
        # Process the data
        process_sensor_data(data_dict)
        
        return {
            "status": "success",
            "message": f"Data ingested for {data.asset_id}",
            "asset_id": data.asset_id,
            "risk_multiplier": data_dict.get('risk_multiplier', 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/batch", tags=["Real-Time"])
def ingest_batch_sensor_data(batch: BatchIngest):
    """
    Ingest multiple sensor data points at once
    
    Send a batch of sensor readings for multiple assets.
    Returns a summary of successful and failed ingestions.
    """
    try:
        batch_id = batch.batch_id or str(uuid.uuid4())
        results = []
        success_count = 0
        
        for data in batch.data:
            data_dict = data.dict()
            if not data_dict.get('timestamp'):
                data_dict['timestamp'] = datetime.now().isoformat()
            
            success = process_sensor_data(data_dict)
            results.append({
                "asset_id": data.asset_id,
                "success": success
            })
            if success:
                success_count += 1
        
        return {
            "status": "success",
            "batch_id": batch_id,
            "total": len(batch.data),
            "successful": success_count,
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/realtime/latest", tags=["Real-Time"])
def get_latest_sensor_data(asset_id: Optional[str] = None):
    """
    Get latest sensor data for asset(s)
    
    Returns the most recent sensor readings including:
    - Temperature, vibration, pressure
    - Engine hours, fuel level
    - Error codes
    - Calculated risk multiplier
    
    If asset_id is provided, returns data for that asset only.
    Otherwise, returns data for all assets.
    """
    try:
        if asset_id:
            data = realtime_asset_data.get(asset_id)
            if not data:
                raise HTTPException(status_code=404, detail=f"Asset {asset_id} not found")
            return {
                "status": "success",
                "asset_id": asset_id,
                "data": data,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "status": "success",
                "data": realtime_asset_data,
                "total_assets": len(realtime_asset_data),
                "timestamp": datetime.now().isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/realtime/status", tags=["Real-Time"])
def get_realtime_status():
    """
    Get real-time ingestion system status
    
    Returns:
    - Simulation status (running/stopped)
    - Number of active assets
    - Last update timestamp
    """
    return {
        "status": "running" if simulation_running else "stopped",
        "active_assets": len(realtime_asset_data),
        "last_update": datetime.now().isoformat()
    }

@app.post("/realtime/control", tags=["Real-Time"])
def control_realtime_simulation(action: str):
    """
    Control real-time simulation (start/stop)
    
    Actions:
    - 'start': Begin automatic sensor data generation (every 3 seconds)
    - 'stop': Stop the simulation
    
    Simulation generates realistic sensor data for all 6 assets
    with random temperature, vibration, and occasional error codes.
    """
    if action.lower() == "start":
        if not simulation_running:
            start_simulation()
            return {"status": "success", "message": "Real-time simulation started"}
        return {"status": "info", "message": "Simulation already running"}
    elif action.lower() == "stop":
        if simulation_running:
            stop_simulation()
            return {"status": "success", "message": "Real-time simulation stopped"}
        return {"status": "info", "message": "Simulation not running"}
    else:
        raise HTTPException(status_code=400, detail="Action must be 'start' or 'stop'")

@app.post("/realtime/trigger", tags=["Alerts"])
def trigger_manual_alert(asset_id: str, alert_type: str = "warning", message: str = None):
    """
    Manually trigger an alert for testing
    
    Creates an alert in the database and optionally sends an email
    if the alert system is configured.
    """
    try:
        alert_msg = message or f"Manual alert triggered for {asset_id}"
        
        # Save to database
        db_manager.save_alert(alert_type.upper(), asset_id, alert_msg)
        
        # Try to send email if configured
        try:
            alert_system.send_alert(
                subject=f"Manual Alert: {asset_id}",
                message=alert_msg,
                alert_type=alert_type
            )
        except:
            pass
        
        return {
            "status": "success",
            "message": f"Alert triggered for {asset_id}",
            "alert_type": alert_type
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
