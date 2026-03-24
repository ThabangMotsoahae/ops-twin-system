import pandas as pd
from .transitions import simulate_transitions
from risk_model.risk_engine import compute_risk

# Define thresholds for states
def determine_state(row):
    if row["failure_count"] >= 6 or row["downtime_hours"] > 15:
        return "CRITICAL"
    elif row["failure_count"] >= 3 or row["downtime_hours"] > 8:
        return "WARNING"
    elif row["failure_count"] == 0:
        return "HEALTHY"
    else:
        return "HEALTHY"

# Function to assign states to all assets
def assign_states(data_path):
    df = pd.read_csv(data_path)
    df["state"] = df.apply(determine_state, axis=1)
    return df

# Main execution
if __name__ == "__main__":
    # Load and assign initial states
    df = assign_states("data/assets.csv")
    print("\n=== Initial States ===")
    print(df[["asset_id", "state", "failure_count", "downtime_hours"]])
    
    # Simulate transitions over 5 steps
    print("\n=== Simulating State Transitions ===")
    df_history = simulate_transitions(df, steps=5)
    print(df_history[["asset_id", "state", "step", "failure_count", "downtime_hours"]].head(10))
    
    # Compute risk scores
    print("\n=== Risk Scores Over Time ===")
    df_risk = compute_risk(df_history)
    print(df_risk[["asset_id", "state", "step", "risk_score"]].head(10))
    
    # Summary statistics
    print("\n=== Summary ===")
    print(f"Total simulations: {len(df_risk)} records")
    print(f"Unique assets: {df_risk['asset_id'].nunique()}")
    print(f"Risk score range: {df_risk['risk_score'].min():.2f} - {df_risk['risk_score'].max():.2f}")


