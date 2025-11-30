"""
=============================================================================
SNS SERVICE - Amazon Simple Notification Service Integration
=============================================================================
Student: Raushan Kumar
Course: Cloud Computing / AWS

What is SNS?
------------
Amazon SNS (Simple Notification Service) is a managed messaging service that:
- Sends messages to multiple subscribers at once
- Supports multiple protocols: Email, SMS, HTTP, Lambda, SQS
- Handles all the complexity of message delivery
- Scales automatically

In this application, we use SNS to:
- Send email alerts when electricity usage is high
- Notify users about usage spikes
- Send daily/weekly summary reports

Key SNS Concepts:
----------------
1. Topic: A communication channel for messages
   - Publishers send messages TO a topic
   - Subscribers receive messages FROM a topic

2. Subscription: A connection between a topic and an endpoint
   - Email subscriptions require confirmation (click a link)
   - Each topic can have multiple subscribers

3. Message: The content sent to subscribers
   - Subject: Email subject line (for email subscribers)
   - Body: The actual message content

Flow:
-----
[Your App] --> [SNS Topic] --> [Email Subscriber 1]
                          --> [Email Subscriber 2]
                          --> [SMS Subscriber]

Our SNS Setup:
--------------
Topic: ElectricityAlerts
ARN: arn:aws:sns:us-east-1:904013368830:ElectricityAlerts

Email alerts include:
- High usage warnings
- Spike detection notifications
- Daily summaries
=============================================================================
"""

# boto3 - AWS SDK for Python
import boto3

# ClientError - Exception class for AWS API errors
from botocore.exceptions import ClientError

# os - For reading environment variables
import os

# typing - For type hints
from typing import Optional, List, Dict


class SNSService:
    """
    A service class for sending notifications via Amazon SNS.
    
    This class provides methods to:
    - Create SNS topics
    - Subscribe email addresses
    - Send various types of alerts
    - List subscriptions
    
    Usage:
        sns = SNSService()
        sns.create_topic_if_not_exists()
        sns.subscribe_email("user@example.com")
        sns.send_alert("Alert!", "High usage detected")
    """
    
    def __init__(self, topic_arn: str = None):
        """
        Initialize the SNS service.
        
        Args:
            topic_arn: Optional pre-existing topic ARN.
                      If not provided, will create or find topic by name.
        
        Environment Variables Used:
        - SNS_TOPIC_ARN: The ARN of an existing topic
        - SNS_TOPIC_NAME: Name for creating new topic
        - AWS credentials (ACCESS_KEY, SECRET_KEY, SESSION_TOKEN)
        """
        # Get topic ARN from parameter or environment
        self.topic_arn = topic_arn or os.getenv('SNS_TOPIC_ARN')
        
        # Topic name for creating new topics
        self.topic_name = os.getenv('SNS_TOPIC_NAME', 'ElectricityAlerts')
        
        # AWS region
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        
        # Session token for Learner Lab
        session_token = os.getenv('AWS_SESSION_TOKEN')
        
        # Create SNS client
        self.sns_client = boto3.client(
            'sns',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=session_token if session_token else None
        )
    
    def create_topic_if_not_exists(self) -> Optional[str]:
        """
        Create an SNS topic if it doesn't exist.
        
        Note: create_topic is idempotent - if the topic already exists,
        it simply returns the existing topic's ARN.
        
        Returns:
            str: The topic ARN, or None if creation failed
        
        What is an ARN?
        ---------------
        ARN = Amazon Resource Name
        Format: arn:aws:sns:region:account-id:topic-name
        Example: arn:aws:sns:us-east-1:904013368830:ElectricityAlerts
        
        ARNs uniquely identify AWS resources across all accounts.
        """
        try:
            # Create topic (returns existing topic if already exists)
            response = self.sns_client.create_topic(Name=self.topic_name)
            
            # Store the ARN for later use
            self.topic_arn = response['TopicArn']
            print(f"SNS topic ready: {self.topic_arn}")
            return self.topic_arn
            
        except ClientError as e:
            print(f"Failed to create SNS topic: {e}")
            return None
    
    def subscribe_email(self, email: str) -> Optional[str]:
        """
        Subscribe an email address to receive alerts.
        
        Important: After subscribing, AWS sends a confirmation email.
        The user MUST click the confirmation link in that email!
        Until confirmed, the subscription status is "PendingConfirmation".
        
        Args:
            email: The email address to subscribe
        
        Returns:
            str: The subscription ARN (or 'PendingConfirmation')
        
        Example:
            arn = sns.subscribe_email("user@example.com")
            # Returns: "pending confirmation" or actual subscription ARN
        """
        if not self.topic_arn:
            print("No topic ARN configured")
            return None
        
        try:
            response = self.sns_client.subscribe(
                TopicArn=self.topic_arn,
                Protocol='email',      # Using email protocol
                Endpoint=email         # The email address
            )
            return response['SubscriptionArn']
            
        except ClientError as e:
            print(f"Failed to subscribe email: {e}")
            return None
    
    def list_subscriptions(self) -> List[Dict]:
        """
        List all subscriptions for the topic.
        
        Returns information about each subscriber including:
        - SubscriptionArn: Unique identifier
        - Protocol: email, sms, http, etc.
        - Endpoint: The actual email/phone/URL
        - Status: Confirmed or PendingConfirmation
        
        Returns:
            list: List of subscription dictionaries
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
        Send an alert to all topic subscribers.
        
        This is the core method for sending notifications.
        All confirmed subscribers will receive the message.
        
        Args:
            subject: Email subject line (max 100 characters)
            message: The message body (can be multi-line)
        
        Returns:
            bool: True if message was published successfully
        
        Example:
            sns.send_alert(
                subject="⚡ High Usage Alert",
                message="Your electricity usage exceeded 10 kWh today!"
            )
        """
        if not self.topic_arn:
            print("No topic ARN configured")
            return False
        
        try:
            # Publish message to the topic
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
        Send an alert when usage exceeds the threshold.
        
        This is a convenience method that formats a usage alert message.
        
        Args:
            device_id: The device/meter ID
            current_kwh: Current usage in kWh
            threshold_kwh: The threshold that was exceeded
        
        Returns:
            bool: True if alert was sent successfully
        
        Example Email:
            Subject: ⚡ High Electricity Usage Alert - device-001
            
            🔌 Electricity Usage Alert
            
            Device ID: device-001
            Current Usage: 15.50 kWh
            Threshold: 10.00 kWh
            
            Your electricity consumption has exceeded the set threshold!
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
    
    def send_spike_alert(self, device_id: str, date: str, prev_kwh: float, 
                         curr_kwh: float, change_pct: float) -> bool:
        """
        Send an alert when a usage spike is detected.
        
        A spike is a sudden, significant increase in usage
        compared to the previous period.
        
        Args:
            device_id: The device/meter ID
            date: Date when spike occurred
            prev_kwh: Previous period's usage
            curr_kwh: Current period's usage
            change_pct: Percentage increase
        
        Returns:
            bool: True if alert was sent successfully
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
    
    def send_daily_summary(self, device_id: str, date: str, total_kwh: float, 
                           cost: float, currency: str = "EUR") -> bool:
        """
        Send a daily usage summary email.
        
        This can be called at the end of each day to provide
        users with a summary of their electricity consumption.
        
        Args:
            device_id: The device/meter ID
            date: The date for the summary
            total_kwh: Total usage for the day
            cost: Estimated cost
            currency: Currency code (default: EUR)
        
        Returns:
            bool: True if summary was sent successfully
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
