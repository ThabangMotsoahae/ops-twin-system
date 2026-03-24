import asyncio
import json
import websockets
from typing import Set
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from data_ingestion.stream_ingestor import ingestor

class WebSocketServer:
    def __init__(self):
        self.connected_clients: Set[websockets.WebSocketServerProtocol] = set()
        self.is_running = False
    
    async def handler(self, websocket, path):
        """Handle WebSocket connections"""
        self.connected_clients.add(websocket)
        print(f"🔌 Client connected. Total clients: {len(self.connected_clients)}")
        
        try:
            async for message in websocket:
                # Parse incoming message
                data = json.loads(message)
                
                if data.get('type') == 'subscribe':
                    # Client wants to subscribe to real-time updates
                    await self.send_realtime_updates(websocket)
                elif data.get('type') == 'ingest':
                    # Client is sending sensor data
                    ingestor.process_incoming_data(data.get('data', {}))
                    await websocket.send(json.dumps({
                        'type': 'ack',
                        'status': 'received',
                        'timestamp': data.get('timestamp')
                    }))
                    
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self.connected_clients.remove(websocket)
            print(f"🔌 Client disconnected. Total clients: {len(self.connected_clients)}")
    
    async def send_realtime_updates(self, websocket):
        """Send real-time updates to subscribed client"""
        while websocket in self.connected_clients:
            # Get latest data
            latest_data = ingestor.get_latest_data()
            
            # Send to client
            await websocket.send(json.dumps({
                'type': 'update',
                'timestamp': __import__('datetime').datetime.now().isoformat(),
                'data': latest_data
            }))
            
            await asyncio.sleep(2)  # Update every 2 seconds
    
    async def start_server(self, host='localhost', port=8765):
        """Start WebSocket server"""
        self.is_running = True
        async with websockets.serve(self.handler, host, port):
            print(f"✅ WebSocket server running on ws://{host}:{port}")
            await asyncio.Future()  # Run forever

# Create global instance
ws_server = WebSocketServer()

if __name__ == "__main__":
    # Run WebSocket server
    asyncio.run(ws_server.start_server())
