from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import sys
from pathlib import Path
from risk_model.alert_system import alert_system
from risk_model.database_manager import db_manager

# Add parent directory to path so we can import modules
sys.path.append(str(Path(__file__).parent.parent))

from state_engine.state_manager import assign_states
from state_engine.transitions import simulate_transitions
from risk_model.risk_engine import compute_risk

app = FastAPI(
    title="OpsTwin API",
    description="Operational Digital Twin for Risk & Reliability",
    version="1.0.0"
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

# Response models
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

@app.get("/")
def root():
    return {
        "system": "OpsTwin",
        "version": "1.0.0",
        "description": "Operational Digital Twin for Risk & Reliability",
        "endpoints": [
            "/states",
            "/risk/{asset_id}",
            "/simulate/{steps}",
            "/high-risk/{threshold}"
        ]
    }

@app.get("/states", response_model=list[AssetState])
def get_current_states():
    """
    Get current states for all assets
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

@app.get("/risk/{asset_id}")
def get_asset_risk(asset_id: str):
    """
    Get risk score for a specific asset
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

@app.get("/simulate/{steps}", response_model=list[RiskResponse])
def simulate_future(steps: int):
    """
    Simulate state transitions for a number of steps
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

@app.get("/high-risk/{threshold}")
def get_high_risk_assets(threshold: float):
    """
    Get assets with risk score above threshold
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

@app.get("/metrics")
def get_system_metrics():
    """
    Get system-wide metrics
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

@app.post("/configure-alerts")
def configure_alerts(email_from: str, email_password: str, email_to: list[str]):
    """Configure email alert system"""
    try:
        alert_system.configure_email(email_from, email_password, email_to)
        return {"status": "success", "message": "Email alerts configured"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/alerts/history")
def get_alert_history(limit: int = 50):
    """Get alert history"""
    try:
        df = db_manager.get_alert_history(limit)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/assets/history/{asset_id}")
def get_asset_history(asset_id: str, limit: int = 100):
    """Get historical data for an asset"""
    try:
        df = db_manager.get_history(asset_id, limit)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/save-state")
def save_current_state():
    """Save current asset states to database"""
    try:
        df = assign_states(DATA_PATH)
        df_risk = compute_risk(df)
        db_manager.save_asset_states(df_risk)
        return {"status": "success", "message": "State saved to database"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
