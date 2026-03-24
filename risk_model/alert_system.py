import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pandas as pd

class AlertSystem:
    def __init__(self, smtp_server="smtp.gmail.com", smtp_port=587):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.email_enabled = False
        self.email_from = None
        self.email_password = None
        self.email_to = []
    
    def configure_email(self, email_from, email_password, email_to):
        """Configure email settings"""
        self.email_from = email_from
        self.email_password = email_password
        self.email_to = email_to if isinstance(email_to, list) else [email_to]
        self.email_enabled = True
        print(f"✅ Email alerts configured for {self.email_from} → {self.email_to}")
    
    def send_alert(self, subject, message, alert_type="warning"):
        """Send email alert"""
        if not self.email_enabled:
            print(f"⚠️ Email not configured. Would send: {subject}")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.email_from
            msg['To'] = ', '.join(self.email_to)
            msg['Subject'] = f"[OpsTwin-{alert_type.upper()}] {subject}"
            
            # Add timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            full_message = f"""
            {message}
            
            ⏰ Time: {timestamp}
            🛰️ System: OpsTwin Digital Twin
            
            This is an automated alert from your Operational Digital Twin system.
            """
            
            msg.attach(MIMEText(full_message, 'plain'))
            
            # Send email
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.email_from, self.email_password)
            server.send_message(msg)
            server.quit()
            
            print(f"✅ Alert sent: {subject}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send alert: {e}")
            return False
    
    def check_high_risk_assets(self, df, threshold=0.7):
        """Check for high-risk assets and generate alerts"""
        high_risk = df[df['risk_score'] >= threshold]
        
        if len(high_risk) > 0:
            alert_message = f"🚨 {len(high_risk)} high-risk asset(s) detected:\n\n"
            for _, asset in high_risk.iterrows():
                alert_message += f"• {asset['asset_id']} ({asset['asset_type']}) - Risk: {asset['risk_score']:.2f} - State: {asset['state']}\n"
            
            self.send_alert(
                subject=f"High-Risk Alert: {len(high_risk)} Assets Critical",
                message=alert_message,
                alert_type="critical"
            )
            return True
        return False
    
    def check_state_changes(self, old_df, new_df):
        """Alert on state transitions to critical/failure"""
        alerts = []
        
        for _, new_row in new_df.iterrows():
            asset_id = new_row['asset_id']
            old_row = old_df[old_df['asset_id'] == asset_id]
            
            if not old_row.empty:
                old_state = old_row.iloc[0]['state']
                new_state = new_row['state']
                
                if new_state in ['CRITICAL', 'FAILURE'] and old_state != new_state:
                    alert = f"⚠️ {asset_id} transitioned: {old_state} → {new_state}"
                    alerts.append(alert)
        
        if alerts:
            self.send_alert(
                subject=f"State Transitions: {len(alerts)} Assets Changed",
                message="\n".join(alerts),
                alert_type="warning"
            )
            return True
        return False

# Create global alert instance
alert_system = AlertSystem()

if __name__ == "__main__":
    # Test the alert system
    print("Testing Alert System...")
    
    # Configure with your email (optional - for testing)
    # alert_system.configure_email("your_email@gmail.com", "your_app_password", ["recipient@example.com"])
    
    # Test with sample data
    test_df = pd.DataFrame({
        'asset_id': ['TEST001'],
        'asset_type': ['Truck'],
        'state': ['CRITICAL'],
        'risk_score': [0.85]
    })
    
    alert_system.check_high_risk_assets(test_df)
