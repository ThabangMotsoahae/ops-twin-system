import pandas as pd
import numpy as np

def simulate_transitions(df, steps=5):
    """
    Simulate state transitions over multiple time steps
    Each step, randomly transition states based on probabilities
    """
    # State transition matrix
    TRANSITION_MATRIX = {
        "HEALTHY": {"HEALTHY": 0.85, "WARNING": 0.15, "CRITICAL": 0.0, "FAILURE": 0.0},
        "WARNING": {"HEALTHY": 0.10, "WARNING": 0.70, "CRITICAL": 0.20, "FAILURE": 0.0},
        "CRITICAL": {"HEALTHY": 0.0, "WARNING": 0.15, "CRITICAL": 0.70, "FAILURE": 0.15},
        "FAILURE": {"HEALTHY": 0.0, "WARNING": 0.0, "CRITICAL": 0.20, "FAILURE": 0.80},
    }
    
    def next_state(current_state):
        """Get next state based on transition probabilities"""
        if current_state not in TRANSITION_MATRIX:
            return current_state
        probs = TRANSITION_MATRIX[current_state]
        states = list(probs.keys())
        probabilities = list(probs.values())
        return np.random.choice(states, p=probabilities)
    
    # Create history
    history = []
    current_df = df.copy()
    
    for step in range(1, steps + 1):
        current_df = current_df.copy()
        current_df["step"] = step
        history.append(current_df)
        
        # Transition to next state (except for last step)
        if step < steps:
            current_df["state"] = current_df["state"].apply(next_state)
    
    return pd.concat(history, ignore_index=True)
