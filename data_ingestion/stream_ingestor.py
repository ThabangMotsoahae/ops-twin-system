import asyncio
import json
import time
import random
from datetime import datetime
from typing import Dict, List, Optional
import threading
import queue

class RealTimeDataIngestor:
    """
    Real-time data ingestion engine for OpsTwin
    Supports WebSocket, MQTT, and REST batch ingestion
    """
    
    def __init__(self):
        self.data_queue = queue.Queue()
        self.active_connections = []
        self.asset_data = {}
        self.is_running = False
        self.simulation_mode = True
        
    def start_simulation(self, interval_seconds=5):
        """Simulate real-time sensor data for demo purposes"""
        self.is_running = True
        
        def simulate():
            while self.is_running:
                # Generate simulated sensor data
                for asset_id in ['TRK001', 'TRK002', 'DRL001', 'DRL002', 'LDR001', 'LDR002']:
                    data = {
                        'asset_id': asset_id,
                        'timestamp': datetime.now().isoformat(),
                        'temperature': round(random.uniform(20, 80), 1),
                        'vibration': round(random.uniform(0, 10), 2),
                        'pressure': round(random.uniform(90, 110), 1),
                        'engine_hours': random.randint(0, 24),
                        'fuel_level': round(random.uniform(0, 100), 1),
                        'error_code': random.choice([None, 'E101', 'E202', 'E303', None, None])
                    }
                    
                    # Add to queue
                    self.data_queue.put(data)
                    
                    # Store latest data
                    self.asset_data[asset_id] = data
                
                time.sleep(interval_seconds)
        
        thread = threading.Thread(target=simulate, daemon=True)
        thread.start()
        print(f"✅ Real-time data simulation started (every {interval_seconds}s)")
    
    def stop_simulation(self):
        """Stop the data simulation"""
        self.is_running = False
        print("✅ Data simulation stopped")
    
    def process_incoming_data(self, data: Dict):
        """Process incoming sensor data and update asset state"""
        try:
            asset_id = data.get('asset_id')
            if not asset_id:
                return False
            
            # Store raw data
            self.asset_data[asset_id] = data
            
            # Calculate derived metrics
            risk_multiplier = 0
            
            # Temperature factor (high temp = higher risk)
            temp = data.get('temperature', 20)
            if temp > 70:
                risk_multiplier += 0.3
            elif temp > 50:
                risk_multiplier += 0.1
            
            # Vibration factor
            vibration = data.get('vibration', 0)
            if vibration > 8:
                risk_multiplier += 0.4
            elif vibration > 5:
                risk_multiplier += 0.2
            
            # Error code factor
            if data.get('error_code'):
                risk_multiplier += 0.5
            
            # Fuel level factor
            fuel = data.get('fuel_level', 100)
            if fuel < 10:
                risk_multiplier += 0.3
            
            # Update asset risk score
            data['risk_multiplier'] = min(1.0, risk_multiplier)
            
            # Trigger alert if needed
            if risk_multiplier > 0.7:
                self.trigger_alert(asset_id, data)
            
            print(f"📡 Processed data for {asset_id}: Risk multiplier = {risk_multiplier:.2f}")
            return True
            
        except Exception as e:
            print(f"❌ Error processing data: {e}")
            return False
    
    def trigger_alert(self, asset_id: str, data: Dict):
        """Trigger alert for high-risk asset"""
        alert = {
            'asset_id': asset_id,
            'timestamp': datetime.now().isoformat(),
            'type': 'CRITICAL',
            'message': f"Real-time anomaly detected: Temp={data.get('temperature')}°C, Vib={data.get('vibration')}, Error={data.get('error_code')}",
            'data': data
        }
        
        print(f"🚨 ALERT: {alert['message']}")
        return alert
    
    def get_latest_data(self, asset_id: Optional[str] = None):
        """Get latest sensor data for asset(s)"""
        if asset_id:
            return self.asset_data.get(asset_id)
        return self.asset_data

# Global ingestor instance
ingestor = RealTimeDataIngestor()

if __name__ == "__main__":
    # Test the ingestor
    print("Testing Real-Time Data Ingestor...")
    ingestor.start_simulation(interval_seconds=2)
    
    try:
        time.sleep(10)
        print(f"Latest data: {ingestor.get_latest_data('TRK001')}")
    finally:
        ingestor.stop_simulation()
