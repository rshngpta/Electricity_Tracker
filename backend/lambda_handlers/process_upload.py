# backend/lambda_handlers/process_upload.py
"""
Lambda function to process CSV uploads from S3
Triggered when a new CSV file is uploaded to the S3 bucket
"""
import json
import boto3
import os
from datetime import datetime
from decimal import Decimal
import csv
from io import StringIO

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Get table name from environment
TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME', 'ElectricityReadings')


def lambda_handler(event, context):
    """
    Process CSV file uploaded to S3 and store readings in DynamoDB.
    
    Triggered by S3 PUT event.
    """
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Get bucket and key from S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"Processing file: s3://{bucket}/{key}")
        
        # Download the CSV file
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Parse CSV
        readings = parse_csv(csv_content)
        print(f"Parsed {len(readings)} readings")
        
        # Store in DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        stored_count = 0
        
        with table.batch_writer() as batch:
            for reading in readings:
                batch.put_item(Item={
                    'device_id': reading['device_id'],
                    'timestamp': reading['timestamp'],
                    'kwh': Decimal(str(reading['kwh'])),
                    'source_file': key,
                    'processed_at': datetime.utcnow().isoformat()
                })
                stored_count += 1
        
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully processed CSV',
                'file': key,
                'readings_count': len(readings),
                'stored_count': stored_count
            })
        }
        print(f"Result: {result}")
        return result
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


def parse_csv(csv_text: str) -> list:
    """Parse CSV text and return list of readings."""
    f = StringIO(csv_text.strip())
    reader = csv.DictReader(f)
    readings = []
    
    for row in reader:
        if not row.get('device_id') or not row.get('timestamp') or not row.get('kwh'):
            continue
        
        # Handle Z suffix in timestamp
        timestamp = row['timestamp'].replace('Z', '+00:00')
        
        readings.append({
            'device_id': row['device_id'],
            'timestamp': timestamp,
            'kwh': float(row['kwh'])
        })
    
    return readings

