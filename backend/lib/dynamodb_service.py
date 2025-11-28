# backend/lib/dynamodb_service.py
"""
DynamoDB Service for Electricity Tracker
Handles storing and retrieving electricity readings from AWS DynamoDB
"""
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime
from typing import Optional, List, Dict
from decimal import Decimal


class DynamoDBService:
    def __init__(self, table_name: str = None):
        """
        Initialize DynamoDB service.
        
        Uses AWS credentials from environment variables:
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY
        - AWS_SESSION_TOKEN (for Learner Lab)
        - AWS_REGION
        """
        self.table_name = table_name or os.getenv('DYNAMODB_TABLE_NAME', 'ElectricityReadings')
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        
        session_token = os.getenv('AWS_SESSION_TOKEN')
        
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=session_token if session_token else None
        )
        
        self.client = boto3.client(
            'dynamodb',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=session_token if session_token else None
        )
        
        self.table = None
    
    def create_table_if_not_exists(self) -> bool:
        """Create the DynamoDB table if it doesn't exist."""
        try:
            # Check if table exists
            self.client.describe_table(TableName=self.table_name)
            self.table = self.dynamodb.Table(self.table_name)
            print(f"DynamoDB table '{self.table_name}' exists")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                try:
                    # Create table
                    table = self.dynamodb.create_table(
                        TableName=self.table_name,
                        KeySchema=[
                            {'AttributeName': 'device_id', 'KeyType': 'HASH'},  # Partition key
                            {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}   # Sort key
                        ],
                        AttributeDefinitions=[
                            {'AttributeName': 'device_id', 'AttributeType': 'S'},
                            {'AttributeName': 'timestamp', 'AttributeType': 'S'}
                        ],
                        BillingMode='PAY_PER_REQUEST'  # On-demand pricing (good for Learner Lab)
                    )
                    # Wait for table to be created
                    table.wait_until_exists()
                    self.table = table
                    print(f"Created DynamoDB table '{self.table_name}'")
                    return True
                except ClientError as create_error:
                    print(f"Failed to create table: {create_error}")
                    return False
            else:
                print(f"Error checking table: {e}")
                return False
    
    def put_reading(self, device_id: str, timestamp: str, kwh: float) -> bool:
        """
        Store a single reading in DynamoDB.
        
        Args:
            device_id: The device/meter ID
            timestamp: ISO format timestamp
            kwh: Energy consumption in kWh
            
        Returns:
            True if successful, False otherwise
        """
        if not self.table:
            self.table = self.dynamodb.Table(self.table_name)
        
        try:
            self.table.put_item(
                Item={
                    'device_id': device_id,
                    'timestamp': timestamp,
                    'kwh': Decimal(str(kwh)),
                    'created_at': datetime.utcnow().isoformat()
                }
            )
            return True
        except ClientError as e:
            print(f"Failed to put reading: {e}")
            return False
    
    def put_readings_batch(self, readings: List[Dict]) -> int:
        """
        Store multiple readings in DynamoDB using batch write.
        
        Args:
            readings: List of dicts with device_id, timestamp, kwh
            
        Returns:
            Number of successfully written items
        """
        if not self.table:
            self.table = self.dynamodb.Table(self.table_name)
        
        success_count = 0
        
        # DynamoDB batch_write_item can handle max 25 items at a time
        batch_size = 25
        for i in range(0, len(readings), batch_size):
            batch = readings[i:i + batch_size]
            
            try:
                with self.table.batch_writer() as writer:
                    for reading in batch:
                        writer.put_item(Item={
                            'device_id': reading['device_id'],
                            'timestamp': reading['timestamp'],
                            'kwh': Decimal(str(reading['kwh'])),
                            'created_at': datetime.utcnow().isoformat()
                        })
                success_count += len(batch)
            except ClientError as e:
                print(f"Batch write error: {e}")
        
        return success_count
    
    def get_readings_for_device(self, device_id: str) -> List[Dict]:
        """
        Get all readings for a specific device.
        
        Args:
            device_id: The device/meter ID
            
        Returns:
            List of reading dictionaries
        """
        if not self.table:
            self.table = self.dynamodb.Table(self.table_name)
        
        try:
            response = self.table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('device_id').eq(device_id)
            )
            
            readings = []
            for item in response.get('Items', []):
                readings.append({
                    'device_id': item['device_id'],
                    'timestamp': item['timestamp'],
                    'kwh': float(item['kwh'])
                })
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self.table.query(
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('device_id').eq(device_id),
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for item in response.get('Items', []):
                    readings.append({
                        'device_id': item['device_id'],
                        'timestamp': item['timestamp'],
                        'kwh': float(item['kwh'])
                    })
            
            return readings
        except ClientError as e:
            print(f"Failed to get readings: {e}")
            return []
    
    def delete_reading(self, device_id: str, timestamp: str) -> bool:
        """
        Delete a specific reading.
        
        Args:
            device_id: The device/meter ID
            timestamp: The timestamp of the reading
            
        Returns:
            True if successful, False otherwise
        """
        if not self.table:
            self.table = self.dynamodb.Table(self.table_name)
        
        try:
            self.table.delete_item(
                Key={
                    'device_id': device_id,
                    'timestamp': timestamp
                }
            )
            return True
        except ClientError as e:
            print(f"Failed to delete reading: {e}")
            return False
    
    def get_all_devices(self) -> List[str]:
        """
        Get all unique device IDs in the table.
        
        Returns:
            List of device IDs
        """
        if not self.table:
            self.table = self.dynamodb.Table(self.table_name)
        
        try:
            response = self.table.scan(
                ProjectionExpression='device_id'
            )
            
            devices = set()
            for item in response.get('Items', []):
                devices.add(item['device_id'])
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    ProjectionExpression='device_id',
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for item in response.get('Items', []):
                    devices.add(item['device_id'])
            
            return list(devices)
        except ClientError as e:
            print(f"Failed to get devices: {e}")
            return []

