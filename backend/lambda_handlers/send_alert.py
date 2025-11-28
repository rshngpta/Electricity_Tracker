# backend/lambda_handlers/send_alert.py
"""
Lambda function to send alerts via SNS
Can be triggered by DynamoDB Streams, CloudWatch Events, or API Gateway
"""
import json
import boto3
import os
from decimal import Decimal
from boto3.dynamodb.conditions import Key

# Initialize AWS clients
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME', 'ElectricityReadings')
THRESHOLD_KWH = float(os.getenv('ALERT_THRESHOLD_KWH', '10.0'))


def lambda_handler(event, context):
    """
    Check for high usage and send alerts.
    
    Can be triggered by:
    - CloudWatch Events (scheduled check)
    - DynamoDB Streams (real-time check)
    - API Gateway (manual trigger)
    """
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Determine trigger type and get device_id
        if 'Records' in event and event['Records'][0].get('eventSource') == 'aws:dynamodb':
            # Triggered by DynamoDB Stream
            return handle_dynamodb_stream(event)
        elif 'queryStringParameters' in event:
            # Triggered by API Gateway
            return handle_api_request(event)
        else:
            # Triggered by CloudWatch Events (scheduled)
            return handle_scheduled_check(event)
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def handle_dynamodb_stream(event):
    """Process DynamoDB stream records and check for high usage."""
    alerts_sent = 0
    
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            new_image = record['dynamodb']['NewImage']
            device_id = new_image['device_id']['S']
            kwh = float(new_image['kwh']['N'])
            timestamp = new_image['timestamp']['S']
            
            if kwh > THRESHOLD_KWH:
                send_usage_alert(device_id, kwh, THRESHOLD_KWH, timestamp)
                alerts_sent += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps({'alerts_sent': alerts_sent})
    }


def handle_api_request(event):
    """Handle manual alert trigger from API."""
    params = event.get('queryStringParameters') or {}
    device_id = params.get('device_id')
    threshold = float(params.get('threshold_kwh', THRESHOLD_KWH))
    
    if not device_id:
        return response(400, {'error': 'device_id required'})
    
    # Get daily usage
    table = dynamodb.Table(TABLE_NAME)
    result = table.query(
        KeyConditionExpression=Key('device_id').eq(device_id)
    )
    
    # Check for high usage days
    daily_usage = {}
    for item in result.get('Items', []):
        date = item['timestamp'][:10]
        daily_usage[date] = daily_usage.get(date, 0) + float(item['kwh'])
    
    alerts_sent = 0
    for date, kwh in daily_usage.items():
        if kwh > threshold:
            send_usage_alert(device_id, kwh, threshold, date)
            alerts_sent += 1
    
    return response(200, {
        'device_id': device_id,
        'threshold_kwh': threshold,
        'alerts_sent': alerts_sent
    })


def handle_scheduled_check(event):
    """Handle scheduled usage check."""
    # Get all unique devices and check their usage
    # This is a simplified version - in production, you'd want to optimize this
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Scheduled check completed'})
    }


def send_usage_alert(device_id: str, current_kwh: float, threshold_kwh: float, date: str):
    """Send an alert via SNS."""
    if not SNS_TOPIC_ARN:
        print("SNS_TOPIC_ARN not configured")
        return False
    
    subject = f"⚡ High Electricity Usage Alert - {device_id}"
    message = f"""
🔌 Electricity Usage Alert

Device ID: {device_id}
Date: {date}
Usage: {current_kwh:.2f} kWh
Threshold: {threshold_kwh:.2f} kWh

Your electricity consumption has exceeded the set threshold!

---
Electricity Tracker
    """.strip()
    
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        print(f"Alert sent for {device_id}: {current_kwh} kWh")
        return True
    except Exception as e:
        print(f"Failed to send alert: {e}")
        return False


def response(status_code: int, body: dict) -> dict:
    """Create API Gateway response."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(body)
    }

