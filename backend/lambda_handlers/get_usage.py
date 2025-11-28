# backend/lambda_handlers/get_usage.py
"""
Lambda function to get electricity usage data
Triggered by API Gateway
"""
import json
import boto3
import os
from collections import defaultdict
from boto3.dynamodb.conditions import Key

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME', 'ElectricityReadings')


def lambda_handler(event, context):
    """
    Get usage data for a device.
    
    Query parameters:
    - device_id: Required, the device ID
    - period: 'day' or 'month' (default: 'day')
    """
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Get query parameters
        params = event.get('queryStringParameters') or {}
        device_id = params.get('device_id')
        period = params.get('period', 'day')
        
        if not device_id:
            return response(400, {'error': 'device_id is required'})
        
        # Query DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        result = table.query(
            KeyConditionExpression=Key('device_id').eq(device_id)
        )
        
        readings = result.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in result:
            result = table.query(
                KeyConditionExpression=Key('device_id').eq(device_id),
                ExclusiveStartKey=result['LastEvaluatedKey']
            )
            readings.extend(result.get('Items', []))
        
        # Aggregate by period
        if period == 'month':
            usage = aggregate_monthly(readings)
        else:
            usage = aggregate_daily(readings)
        
        # Format response
        data = [{'period': k, 'total_kwh': v} for k, v in sorted(usage.items())]
        
        return response(200, {
            'device_id': device_id,
            'period': period,
            'data': data
        })
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return response(500, {'error': str(e)})


def aggregate_daily(readings: list) -> dict:
    """Aggregate readings by day."""
    daily = defaultdict(float)
    for r in readings:
        date = r['timestamp'][:10]  # YYYY-MM-DD
        daily[date] += float(r['kwh'])
    return dict(daily)


def aggregate_monthly(readings: list) -> dict:
    """Aggregate readings by month."""
    monthly = defaultdict(float)
    for r in readings:
        month = r['timestamp'][:7]  # YYYY-MM
        monthly[month] += float(r['kwh'])
    return dict(monthly)


def response(status_code: int, body: dict) -> dict:
    """Create API Gateway response."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        },
        'body': json.dumps(body)
    }

