# backend/lambda_handlers/estimate_bill.py
"""
Lambda function to estimate electricity bill
Triggered by API Gateway
"""
import json
import boto3
import os
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from boto3.dynamodb.conditions import Key

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME', 'ElectricityReadings')
DEFAULT_RATE = float(os.getenv('DEFAULT_RATE_PER_KWH', '0.20'))


def lambda_handler(event, context):
    """
    Estimate electricity bill for a device.
    
    Query parameters:
    - device_id: Required, the device ID
    - rate: Rate per kWh (default: 0.20)
    - period: 'day' or 'month' (default: 'day')
    """
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Get query parameters
        params = event.get('queryStringParameters') or {}
        device_id = params.get('device_id')
        rate = float(params.get('rate', DEFAULT_RATE))
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
        
        # Calculate total usage
        total_kwh = sum(float(r['kwh']) for r in readings)
        
        # Calculate cost
        cost = total_kwh * rate
        rounded_cost = float(Decimal(str(cost)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        
        return response(200, {
            'device_id': device_id,
            'period': period,
            'total_kwh': round(total_kwh, 4),
            'rate_per_kwh': rate,
            'estimated_cost': rounded_cost,
            'currency': 'EUR'
        })
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return response(500, {'error': str(e)})


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

