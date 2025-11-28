# backend/lib/sns_service.py
"""
SNS Service for Electricity Tracker
Handles sending notifications via AWS SNS (email, SMS, etc.)
"""
import boto3
from botocore.exceptions import ClientError
import os
from typing import Optional, List, Dict


class SNSService:
    def __init__(self, topic_arn: str = None):
        """
        Initialize SNS service.
        
        Uses AWS credentials from environment variables:
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY
        - AWS_SESSION_TOKEN (for Learner Lab)
        - AWS_REGION
        """
        self.topic_arn = topic_arn or os.getenv('SNS_TOPIC_ARN')
        self.topic_name = os.getenv('SNS_TOPIC_NAME', 'ElectricityAlerts')
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        
        session_token = os.getenv('AWS_SESSION_TOKEN')
        
        self.sns_client = boto3.client(
            'sns',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=session_token if session_token else None
        )
    
    def create_topic_if_not_exists(self) -> Optional[str]:
        """
        Create SNS topic if it doesn't exist.
        Returns the topic ARN.
        """
        try:
            # Create topic (idempotent - returns existing topic if already exists)
            response = self.sns_client.create_topic(Name=self.topic_name)
            self.topic_arn = response['TopicArn']
            print(f"SNS topic ready: {self.topic_arn}")
            return self.topic_arn
        except ClientError as e:
            print(f"Failed to create SNS topic: {e}")
            return None
    
    def subscribe_email(self, email: str) -> Optional[str]:
        """
        Subscribe an email address to the topic.
        User will receive a confirmation email they must click to confirm.
        
        Args:
            email: Email address to subscribe
            
        Returns:
            Subscription ARN (pending confirmation) or None if failed
        """
        if not self.topic_arn:
            print("No topic ARN configured")
            return None
        
        try:
            response = self.sns_client.subscribe(
                TopicArn=self.topic_arn,
                Protocol='email',
                Endpoint=email
            )
            return response['SubscriptionArn']
        except ClientError as e:
            print(f"Failed to subscribe email: {e}")
            return None
    
    def list_subscriptions(self) -> List[Dict]:
        """
        List all subscriptions for the topic.
        
        Returns:
            List of subscription details
        """
        if not self.topic_arn:
            return []
        
        try:
            response = self.sns_client.list_subscriptions_by_topic(
                TopicArn=self.topic_arn
            )
            return response.get('Subscriptions', [])
        except ClientError as e:
            print(f"Failed to list subscriptions: {e}")
            return []
    
    def send_alert(self, subject: str, message: str) -> bool:
        """
        Send an alert notification to all subscribers.
        
        Args:
            subject: Email subject line
            message: Alert message body
            
        Returns:
            True if successful, False otherwise
        """
        if not self.topic_arn:
            print("No topic ARN configured")
            return False
        
        try:
            self.sns_client.publish(
                TopicArn=self.topic_arn,
                Subject=subject,
                Message=message
            )
            return True
        except ClientError as e:
            print(f"Failed to send alert: {e}")
            return False
    
    def send_usage_alert(self, device_id: str, current_kwh: float, threshold_kwh: float) -> bool:
        """
        Send an alert when usage exceeds threshold.
        
        Args:
            device_id: The device/meter ID
            current_kwh: Current usage in kWh
            threshold_kwh: Threshold that was exceeded
            
        Returns:
            True if successful, False otherwise
        """
        subject = f"⚡ High Electricity Usage Alert - {device_id}"
        message = f"""
🔌 Electricity Usage Alert

Device ID: {device_id}
Current Usage: {current_kwh:.2f} kWh
Threshold: {threshold_kwh:.2f} kWh

Your electricity consumption has exceeded the set threshold!

Please check your devices to identify any unusual power consumption.

---
Electricity Tracker App
        """.strip()
        
        return self.send_alert(subject, message)
    
    def send_spike_alert(self, device_id: str, date: str, prev_kwh: float, curr_kwh: float, change_pct: float) -> bool:
        """
        Send an alert when a usage spike is detected.
        
        Args:
            device_id: The device/meter ID
            date: Date of the spike
            prev_kwh: Previous day's usage
            curr_kwh: Current day's usage
            change_pct: Percentage change
            
        Returns:
            True if successful, False otherwise
        """
        subject = f"📈 Electricity Spike Detected - {device_id}"
        message = f"""
⚠️ Electricity Usage Spike Detected

Device ID: {device_id}
Date: {date}

Previous Day: {prev_kwh:.2f} kWh
Current Day: {curr_kwh:.2f} kWh
Increase: {change_pct:.1f}%

A significant increase in electricity usage was detected!

---
Electricity Tracker App
        """.strip()
        
        return self.send_alert(subject, message)
    
    def send_daily_summary(self, device_id: str, date: str, total_kwh: float, cost: float, currency: str = "EUR") -> bool:
        """
        Send a daily usage summary.
        
        Args:
            device_id: The device/meter ID
            date: Date of the summary
            total_kwh: Total usage for the day
            cost: Estimated cost
            currency: Currency code
            
        Returns:
            True if successful, False otherwise
        """
        subject = f"📊 Daily Electricity Summary - {date}"
        message = f"""
📊 Daily Electricity Summary

Device ID: {device_id}
Date: {date}

Total Usage: {total_kwh:.2f} kWh
Estimated Cost: {cost:.2f} {currency}

---
Electricity Tracker App
        """.strip()
        
        return self.send_alert(subject, message)

