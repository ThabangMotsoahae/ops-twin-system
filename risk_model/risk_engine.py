import pandas as pd

# Risk weights for each state
RISK_WEIGHTS = {
    "HEALTHY": 0.1,
    "WARNING": 0.5,
    "CRITICAL": 0.8,
    "FAILURE": 1.0
}

def compute_risk(df_history):
    """
    Compute risk score for each asset at each time step
    """
    df = df_history.copy()
    df["risk_score"] = df["state"].apply(lambda x: RISK_WEIGHTS.get(x, 0))
    return df
