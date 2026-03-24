import sqlite3
import pandas as pd
from datetime import datetime
import os

class DatabaseManager:
    def __init__(self, db_path="data/ops_twin.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Asset states history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS asset_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id TEXT,
                asset_type TEXT,
                location TEXT,
                state TEXT,
                failure_count INTEGER,
                downtime_hours INTEGER,
                risk_score REAL,
                timestamp TEXT
            )
        ''')
        
        # Alerts history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT,
                asset_id TEXT,
                message TEXT,
                timestamp TEXT
            )
        ''')
        
        # Simulation results table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS simulations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                simulation_id TEXT,
                step INTEGER,
                asset_id TEXT,
                state TEXT,
                risk_score REAL,
                timestamp TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        print(f"✅ Database initialized at {self.db_path}")
    
    def save_asset_states(self, df):
        """Save current asset states to database"""
        conn = sqlite3.connect(self.db_path)
        df['timestamp'] = datetime.now().isoformat()
        df.to_sql('asset_states', conn, if_exists='append', index=False)
        conn.close()
        print(f"✅ Saved {len(df)} asset states to database")
    
    def save_alert(self, alert_type, asset_id, message):
        """Save alert to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO alerts (alert_type, asset_id, message, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (alert_type, asset_id, message, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        print(f"✅ Alert saved: {alert_type} - {asset_id}")
    
    def save_simulation(self, simulation_id, df_simulation):
        """Save simulation results"""
        conn = sqlite3.connect(self.db_path)
        df_simulation['simulation_id'] = simulation_id
        df_simulation['timestamp'] = datetime.now().isoformat()
        df_simulation.to_sql('simulations', conn, if_exists='append', index=False)
        conn.close()
        print(f"✅ Saved simulation {simulation_id} with {len(df_simulation)} records")
    
    def get_history(self, asset_id=None, limit=100):
        """Get historical data for an asset"""
        conn = sqlite3.connect(self.db_path)
        query = "SELECT * FROM asset_states"
        if asset_id:
            query += f" WHERE asset_id = '{asset_id}'"
        query += f" ORDER BY timestamp DESC LIMIT {limit}"
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def get_alert_history(self, limit=50):
        """Get alert history"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f"SELECT * FROM alerts ORDER BY timestamp DESC LIMIT {limit}", conn)
        conn.close()
        return df

# Create global database instance
db_manager = DatabaseManager()

if __name__ == "__main__":
    # Test database
    print("Testing Database Manager...")
    test_df = pd.DataFrame({
        'asset_id': ['TEST001'],
        'asset_type': ['Truck'],
        'location': ['Section_A'],
        'state': ['HEALTHY'],
        'failure_count': [0],
        'downtime_hours': [0],
        'risk_score': [0.1]
    })
    db_manager.save_asset_states(test_df)
    print(db_manager.get_history())
