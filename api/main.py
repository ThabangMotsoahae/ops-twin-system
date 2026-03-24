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

# ==================== APP CONFIGURATION ====================

app = FastAPI(
    title="OpsTwin API",
    description="Operational Digital Twin for Risk & Reliability",
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
        {"name": "Core", "description": "Asset state and risk management endpoints"},
        {"name": "Real-Time", "description": "Live sensor data ingestion and monitoring"},
        {"name": "Alerts", "description": "Alert configuration and history"},
        {"name": "Database", "description": "Historical data storage and retrieval"}
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

# ==================== CUSTOM SWAGGER UI WITH MODERN DESIGN ====================

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return HTMLResponse(content="""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OpsTwin API | Operational Digital Twin System</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #f5f7fa 0%, #e9ecef 100%);
            min-height: 100vh;
        }
        
        /* Modern Header */
        .hero {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
            padding: 3rem 2rem;
            position: relative;
            overflow: hidden;
        }
        
        .hero::before {
            content: '';
            position: absolute;
            top: -50%;
            right: -20%;
            width: 500px;
            height: 500px;
            background: radial-gradient(circle, rgba(72,187,120,0.1) 0%, transparent 70%);
            border-radius: 50%;
            pointer-events: none;
        }
        
        .hero::after {
            content: '';
            position: absolute;
            bottom: -30%;
            left: -10%;
            width: 400px;
            height: 400px;
            background: radial-gradient(circle, rgba(66,153,225,0.1) 0%, transparent 70%);
            border-radius: 50%;
            pointer-events: none;
        }
        
        .hero-content {
            max-width: 1400px;
            margin: 0 auto;
            position: relative;
            z-index: 1;
        }
        
        .hero-badge {
            display: inline-block;
            background: rgba(72,187,120,0.2);
            backdrop-filter: blur(10px);
            padding: 0.5rem 1rem;
            border-radius: 100px;
            font-size: 0.85rem;
            font-weight: 500;
            color: #48bb78;
            margin-bottom: 1.5rem;
            border: 1px solid rgba(72,187,120,0.3);
        }
        
        .hero h1 {
            font-size: 3.5rem;
            font-weight: 800;
            background: linear-gradient(135deg, #fff 0%, #a0aec0 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 1rem;
            letter-spacing: -0.02em;
        }
        
        .hero-subtitle {
            font-size: 1.2rem;
            color: #94a3b8;
            margin-bottom: 2rem;
            max-width: 600px;
            line-height: 1.6;
        }
        
        /* Stats Grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            max-width: 1400px;
            margin: -2rem auto 0;
            padding: 0 2rem;
            position: relative;
            z-index: 2;
        }
        
        .stat-card {
            background: white;
            border-radius: 1rem;
            padding: 1.5rem;
            box-shadow: 0 10px 25px -5px rgba(0,0,0,0.1), 0 8px 10px -6px rgba(0,0,0,0.02);
            transition: transform 0.2s, box-shadow 0.2s;
            border: 1px solid rgba(0,0,0,0.05);
        }
        
        .stat-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 20px 30px -12px rgba(0,0,0,0.15);
        }
        
        .stat-icon {
            font-size: 2rem;
            margin-bottom: 0.75rem;
        }
        
        .stat-value {
            font-size: 2rem;
            font-weight: 800;
            color: #1e293b;
            margin-bottom: 0.25rem;
        }
        
        .stat-label {
            color: #64748b;
            font-size: 0.9rem;
            font-weight: 500;
        }
        
        /* Quick Links */
        .quick-links {
            max-width: 1400px;
            margin: 2rem auto;
            padding: 0 2rem;
            display: flex;
            gap: 1rem;
            flex-wrap: wrap;
        }
        
        .quick-link {
            background: white;
            padding: 0.75rem 1.5rem;
            border-radius: 100px;
            text-decoration: none;
            color: #1e293b;
            font-weight: 500;
            font-size: 0.9rem;
            transition: all 0.2s;
            border: 1px solid #e2e8f0;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .quick-link:hover {
            background: #48bb78;
            color: white;
            border-color: #48bb78;
            transform: translateY(-2px);
        }
        
        /* Feature Cards */
        .features {
            max-width: 1400px;
            margin: 2rem auto;
            padding: 0 2rem;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 1.5rem;
        }
        
        .feature-card {
            background: white;
            border-radius: 1rem;
            padding: 1.5rem;
            border: 1px solid #e2e8f0;
            transition: all 0.2s;
        }
        
        .feature-card:hover {
            border-color: #48bb78;
            box-shadow: 0 4px 12px rgba(72,187,120,0.1);
        }
        
        .feature-icon {
            font-size: 2rem;
            margin-bottom: 1rem;
        }
        
        .feature-title {
            font-size: 1.1rem;
            font-weight: 700;
            color: #1e293b;
            margin-bottom: 0.5rem;
        }
        
        .feature-desc {
            color: #64748b;
            font-size: 0.9rem;
            line-height: 1.5;
        }
        
        /* Swagger Container */
        .swagger-container {
            background: white;
            border-radius: 1rem;
            margin: 2rem auto;
            max-width: 1400px;
            box-shadow: 0 20px 35px -12px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        /* Swagger UI Overrides */
        .swagger-ui {
            font-family: 'Inter', sans-serif !important;
        }
        
        .swagger-ui .topbar {
            display: none;
        }
        
        .swagger-ui .info {
            margin: 0;
            padding: 0;
        }
        
        .swagger-ui .info .title {
            display: none;
        }
        
        .swagger-ui .info .description {
            margin: 0;
        }
        
        .swagger-ui .info .description .markdown p {
            margin: 0;
        }
        
        .swagger-ui .scheme-container {
            background: transparent;
            box-shadow: none;
            padding: 1rem 2rem;
            border-bottom: 1px solid #e2e8f0;
        }
        
        .swagger-ui .opblock-tag {
            background: #f8fafc;
            border-left: 4px solid #48bb78;
            font-weight: 600;
            font-size: 1rem;
            padding: 1rem 1.5rem;
            margin: 0;
            border-radius: 0;
        }
        
        .swagger-ui .opblock {
            border-radius: 0.75rem;
            margin: 1rem 0;
            border: 1px solid #e2e8f0;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
        }
        
        .swagger-ui .opblock.opblock-get .opblock-summary-method {
            background: #48bb78;
        }
        
        .swagger-ui .opblock.opblock-post .opblock-summary-method {
            background: #4299e1;
        }
        
        .swagger-ui .btn.authorize {
            border-color: #48bb78;
            color: #48bb78;
        }
        
        .swagger-ui .btn.authorize svg {
            fill: #48bb78;
        }
        
        .swagger-ui .model-box {
            background: #f8fafc;
            border-radius: 0.5rem;
        }
        
        /* Footer */
        .footer {
            text-align: center;
            padding: 2rem;
            background: #f1f5f9;
            border-top: 1px solid #e2e8f0;
            color: #64748b;
            font-size: 0.875rem;
        }
        
        .footer a {
            color: #48bb78;
            text-decoration: none;
        }
        
        .footer a:hover {
            text-decoration: underline;
        }
        
        @media (max-width: 768px) {
            .hero h1 { font-size: 2rem; }
            .hero-subtitle { font-size: 1rem; }
            .stat-value { font-size: 1.5rem; }
        }
    </style>
</head>
<body>
    <div class="hero">
        <div class="hero-content">
            <div class="hero-badge">
                <i class="fas fa-microchip"></i> AI Systems Architect
            </div>
            <h1>
                <i class="fas fa-satellite-dish"></i> OpsTwin
            </h1>
            <div class="hero-subtitle">
                Operational Digital Twin for Risk & Reliability<br>
                Real-time asset intelligence with AI-powered risk scoring
            </div>
        </div>
    </div>
    
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-microchip"></i></div>
            <div class="stat-value">6</div>
            <div class="stat-label">Active Assets</div>
        </div>
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-chart-line"></i></div>
            <div class="stat-value">3s</div>
            <div class="stat-label">Real-Time Updates</div>
        </div>
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-code-branch"></i></div>
            <div class="stat-value">14</div>
            <div class="stat-label">API Endpoints</div>
        </div>
        <div class="stat-card">
            <div class="stat-icon"><i class="fas fa-brain"></i></div>
            <div class="stat-value">0-1</div>
            <div class="stat-label">Risk Scale</div>
        </div>
    </div>
    
    <div class="quick-links">
        <a href="https://github.com/ThabangMotsoahae/ops-twin-system" target="_blank" class="quick-link">
            <i class="fab fa-github"></i> GitHub Repository
        </a>
        <a href="https://ops-twin-system-1.onrender.com/realtime/status" target="_blank" class="quick-link">
            <i class="fas fa-heartbeat"></i> System Status
        </a>
        <a href="https://ops-twin-system-1.onrender.com/realtime/latest" target="_blank" class="quick-link">
            <i class="fas fa-chart-line"></i> Live Sensor Data
        </a>
        <a href="https://ops-twin-system-1.onrender.com/metrics" target="_blank" class="quick-link">
            <i class="fas fa-chart-simple"></i> System Metrics
        </a>
    </div>
    
    <div class="features">
        <div class="feature-card">
            <div class="feature-icon"><i class="fas fa-sensor"></i></div>
            <div class="feature-title">Real-Time Simulation</div>
            <div class="feature-desc">Automatic sensor data generation every 3 seconds for 6 assets</div>
        </div>
        <div class="feature-card">
            <div class="feature-icon"><i class="fas fa-chart-scatter"></i></div>
            <div class="feature-title">AI Risk Scoring</div>
            <div class="feature-desc">Multi-factor risk calculation: temp, vibration, error codes, fuel level</div>
        </div>
        <div class="feature-card">
            <div class="feature-icon"><i class="fas fa-bell"></i></div>
            <div class="feature-title">Smart Alerts</div>
            <div class="feature-desc">Automatic alerts for high-risk conditions (risk > 0.7)</div>
        </div>
        <div class="feature-card">
            <div class="feature-icon"><i class="fas fa-database"></i></div>
            <div class="feature-title">Historical Tracking</div>
            <div class="feature-desc">SQLite database storing all asset states and alerts</div>
        </div>
    </div>
    
    <div class="swagger-container">
        <div id="swagger-ui"></div>
    </div>
    
    <div class="footer">
        <p>Built with <i class="fas fa-heart" style="color: #48bb78;"></i> by <strong>Thabang Motsoahae</strong> | Axulo Technologies</p>
        <p style="margin-top: 0.5rem; font-size: 0.8rem;">
            <a href="https://github.com/ThabangMotsoahae" target="_blank"><i class="fab fa-github"></i> GitHub</a> &nbsp;|&nbsp;
            <a href="https://www.linkedin.com/in/thabang-motsoahae-10272b96" target="_blank"><i class="fab fa-linkedin"></i> LinkedIn</a> &nbsp;|&nbsp;
            <a href="mailto:thabangmotsoahae@axulo-inc.com"><i class="fas fa-envelope"></i> Contact</a>
        </p>
        <p style="margin-top: 0.5rem; font-size: 0.75rem; opacity: 0.7;">
            Operational Digital Twin System | MIT License | v2.0.0
        </p>
    </div>
    
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
                displayRequestDuration: true,
                syntaxHighlight: {
                    theme: "monokai"
                }
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
    
    risk_multiplier = 0
    
    temp = data.get('temperature')
    if temp is not None:
        if temp > 70:
            risk_multiplier += 0.3
        elif temp > 50:
            risk_multiplier += 0.1
    
    vibration = data.get('vibration')
    if vibration is not None:
        if vibration > 8:
            risk_multiplier += 0.4
        elif vibration > 5:
            risk_multiplier += 0.2
    
    error_code = data.get('error_code')
    if error_code and error_code != 'None' and error_code != 'null':
        risk_multiplier += 0.5
    
    fuel = data.get('fuel_level')
    if fuel is not None:
        if fuel < 10:
            risk_multiplier += 0.3
    
    data['risk_multiplier'] = min(1.0, risk_multiplier)
    data['processed_at'] = datetime.now().isoformat()
    
    realtime_asset_data[asset_id] = data
    
    if risk_multiplier > 0.7:
        alert_msg = f"⚠️ High risk detected for {asset_id}: Temp={temp}°C, Vib={vibration}, Error={error_code}"
        print(alert_msg)
        try:
            db_manager.save_alert("CRITICAL", asset_id, alert_msg)
        except:
            pass
    
    return True

def start_simulation(interval_seconds=3):
    global simulation_running, simulation_thread
    if simulation_running:
        return
    simulation_running = True
    
    def simulate():
        assets = ['TRK001', 'TRK002', 'DRL001', 'DRL002', 'LDR001', 'LDR002']
        while simulation_running:
            for asset_id in assets:
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
                process_sensor_data(data)
            time.sleep(interval_seconds)
    
    simulation_thread = threading.Thread(target=simulate, daemon=True)
    simulation_thread.start()
    print(f"✅ Real-time simulation started (every {interval_seconds}s)")

def stop_simulation():
    global simulation_running
    simulation_running = False
    print("✅ Real-time simulation stopped")

@app.on_event("startup")
async def startup_event():
    start_simulation(interval_seconds=3)

@app.on_event("shutdown")
async def shutdown_event():
    stop_simulation()

# ==================== CORE ENDPOINTS ====================

@app.get("/", tags=["Core"])
def root():
    return {
        "system": "OpsTwin",
        "version": "2.0.0",
        "description": "Operational Digital Twin for Risk & Reliability",
        "endpoints": [
            "/states", "/risk/{asset_id}", "/simulate/{steps}", "/high-risk/{threshold}",
            "/metrics", "/ingest/single", "/ingest/batch", "/realtime/latest",
            "/realtime/status", "/realtime/control", "/configure-alerts",
            "/alerts/history", "/assets/history/{asset_id}", "/save-state"
        ]
    }

@app.get("/states", response_model=list[AssetState], tags=["Core"])
def get_current_states():
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
                {"asset_id": row["asset_id"], "state": row["state"], "risk_score": row["risk_score"]}
                for _, row in high_risk.iterrows()
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics", tags=["Core"])
def get_system_metrics():
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
    try:
        alert_system.configure_email(email_from, email_password, email_to)
        return {"status": "success", "message": "Email alerts configured"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/alerts/history", tags=["Alerts"])
def get_alert_history(limit: int = 50):
    try:
        df = db_manager.get_alert_history(limit)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/assets/history/{asset_id}", tags=["Database"])
def get_asset_history(asset_id: str, limit: int = 100):
    try:
        df = db_manager.get_history(asset_id, limit)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/save-state", tags=["Database"])
def save_current_state():
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
    try:
        data_dict = data.dict()
        if not data_dict.get('timestamp'):
            data_dict['timestamp'] = datetime.now().isoformat()
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
    try:
        batch_id = batch.batch_id or str(uuid.uuid4())
        results = []
        success_count = 0
        for data in batch.data:
            data_dict = data.dict()
            if not data_dict.get('timestamp'):
                data_dict['timestamp'] = datetime.now().isoformat()
            success = process_sensor_data(data_dict)
            results.append({"asset_id": data.asset_id, "success": success})
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
    try:
        if asset_id:
            data = realtime_asset_data.get(asset_id)
            if not data:
                raise HTTPException(status_code=404, detail=f"Asset {asset_id} not found")
            return {"status": "success", "asset_id": asset_id, "data": data, "timestamp": datetime.now().isoformat()}
        else:
            return {"status": "success", "data": realtime_asset_data, "total_assets": len(realtime_asset_data), "timestamp": datetime.now().isoformat()}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/realtime/status", tags=["Real-Time"])
def get_realtime_status():
    return {
        "status": "running" if simulation_running else "stopped",
        "active_assets": len(realtime_asset_data),
        "last_update": datetime.now().isoformat()
    }

@app.post("/realtime/control", tags=["Real-Time"])
def control_realtime_simulation(action: str):
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
    try:
        alert_msg = message or f"Manual alert triggered for {asset_id}"
        db_manager.save_alert(alert_type.upper(), asset_id, alert_msg)
        try:
            alert_system.send_alert(subject=f"Manual Alert: {asset_id}", message=alert_msg, alert_type=alert_type)
        except:
            pass
        return {"status": "success", "message": f"Alert triggered for {asset_id}", "alert_type": alert_type}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
