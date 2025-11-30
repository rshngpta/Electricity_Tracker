"""
=============================================================================
DYNAMODB SERVICE - Amazon DynamoDB (NoSQL Database) Integration
=============================================================================
Student: Raushan Kumar
Course: Cloud Computing / AWS

What is DynamoDB?
-----------------
Amazon DynamoDB is a fully managed NoSQL database service that offers:
- Fast and consistent performance at any scale
- Automatic scaling to handle any traffic
- Built-in security with encryption
- No servers to manage

NoSQL vs SQL:
-------------
SQL (Relational):           NoSQL (DynamoDB):
- Tables with fixed schema  - Flexible schema
- JOINs for relationships   - Denormalized data
- Vertical scaling          - Horizontal scaling
- Good for complex queries  - Good for key-value access

Key DynamoDB Concepts:
---------------------
1. Table: A collection of items (like a table in SQL)
2. Item: A single record (like a row in SQL)
3. Attribute: A data element (like a column in SQL)
4. Primary Key: Unique identifier for each item
   - Partition Key (HASH): Distributes data across partitions
   - Sort Key (RANGE): Orders items within a partition

Our Table Schema:
-----------------
Table: ElectricityReadings
- device_id (String) - Partition Key - Groups readings by device
- timestamp (String) - Sort Key - Orders readings chronologically
- kwh (Number) - The electricity reading value
- created_at (String) - When the record was inserted

Example Item:
{
    "device_id": "device-001",
    "timestamp": "2025-11-01T00:00:00+00:00",
    "kwh": 0.34,
    "created_at": "2025-11-28T10:30:00Z"
}
=============================================================================
"""

# boto3 - AWS SDK for Python
import boto3

# ClientError - Exception class for AWS API errors
from botocore.exceptions import ClientError

# os - For reading environment variables
import os

# datetime - For generating timestamps
from datetime import datetime

# typing - For type hints
from typing import Optional, List, Dict

# Decimal - For precise number handling (DynamoDB uses Decimal, not float)
from decimal import Decimal


class DynamoDBService:
    """
    A service class for interacting with Amazon DynamoDB.
    
    This class provides methods to:
    - Create DynamoDB tables
    - Store electricity readings (single or batch)
    - Query readings for a specific device
    - List all devices
    - Delete readings
    
    Usage:
        db = DynamoDBService()
        db.create_table_if_not_exists()
        db.put_reading("device-001", "2025-11-01T00:00:00", 0.34)
    """
    
    def __init__(self, table_name: str = None):
        """
        Initialize the DynamoDB service.
        
        This constructor:
        1. Reads AWS credentials from environment variables
        2. Creates boto3 DynamoDB resource and client
        
        Args:
            table_name: Optional custom table name. If not provided,
                       uses DYNAMODB_TABLE_NAME from environment or default.
        
        Note on Resource vs Client:
        - Resource: High-level, object-oriented interface
        - Client: Low-level, direct API calls
        We use both because some operations are easier with resource,
        others require the client.
        """
        # Get table name from parameter, environment variable, or use default
        self.table_name = table_name or os.getenv('DYNAMODB_TABLE_NAME', 'ElectricityReadings')
        
        # Get AWS region
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        
        # Get session token for Learner Lab
        session_token = os.getenv('AWS_SESSION_TOKEN')
        
        # Create DynamoDB Resource (high-level interface)
        # The resource provides Table objects with convenient methods
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=session_token if session_token else None
        )
        
        # Create DynamoDB Client (low-level interface)
        # The client is needed for operations like describe_table
        self.client = boto3.client(
            'dynamodb',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=session_token if session_token else None
        )
        
        # Table object - will be set when table is accessed
        self.table = None
    
    def create_table_if_not_exists(self) -> bool:
        """
        Create the DynamoDB table if it doesn't exist.
        
        Table Schema:
        - device_id (String): Partition Key
          Groups all readings from the same device together
        - timestamp (String): Sort Key
          Allows efficient queries by time range
        
        Billing Mode:
        - PAY_PER_REQUEST (On-Demand): Pay only for what you use
          Good for variable workloads and Learner Lab
        - PROVISIONED: Pre-allocate read/write capacity
          Better for predictable, high-volume workloads
        
        Returns:
            bool: True if table exists or was created successfully
        """
        try:
            # Check if table already exists
            self.client.describe_table(TableName=self.table_name)
            # If no exception, table exists
            self.table = self.dynamodb.Table(self.table_name)
            print(f"DynamoDB table '{self.table_name}' exists")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Table doesn't exist, create it
                try:
                    table = self.dynamodb.create_table(
                        TableName=self.table_name,
                        
                        # Define the primary key schema
                        KeySchema=[
                            {
                                'AttributeName': 'device_id',
                                'KeyType': 'HASH'  # Partition key
                            },
                            {
                                'AttributeName': 'timestamp',
                                'KeyType': 'RANGE'  # Sort key
                            }
                        ],
                        
                        # Define the attributes used in key schema
                        AttributeDefinitions=[
                            {
                                'AttributeName': 'device_id',
                                'AttributeType': 'S'  # String
                            },
                            {
                                'AttributeName': 'timestamp',
                                'AttributeType': 'S'  # String
                            }
                        ],
                        
                        # Use on-demand pricing (no capacity planning needed)
                        BillingMode='PAY_PER_REQUEST'
                    )
                    
                    # Wait for table to be fully created
                    # This can take a few seconds
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
        Store a single electricity reading in DynamoDB.
        
        Args:
            device_id: The device/meter ID (e.g., "device-001")
            timestamp: ISO format timestamp (e.g., "2025-11-01T00:00:00+00:00")
            kwh: Energy consumption in kilowatt-hours
        
        Returns:
            bool: True if successful, False otherwise
        
        Note:
            DynamoDB requires Decimal for numbers, not float.
            We convert using str(kwh) to avoid floating-point precision issues.
        
        Example:
            db.put_reading("device-001", "2025-11-01T00:00:00", 0.34)
        """
        # Ensure we have a table reference
        if not self.table:
            self.table = self.dynamodb.Table(self.table_name)
        
        try:
            # Insert the item into the table
            self.table.put_item(
                Item={
                    'device_id': device_id,
                    'timestamp': timestamp,
                    # Convert float to Decimal via string to avoid precision loss
                    'kwh': Decimal(str(kwh)),
                    # Add metadata for tracking
                    'created_at': datetime.utcnow().isoformat()
                }
            )
            return True
            
        except ClientError as e:
            print(f"Failed to put reading: {e}")
            return False
    
    def put_readings_batch(self, readings: List[Dict]) -> int:
        """
        Store multiple readings efficiently using batch write.
        
        Batch writes are more efficient than individual puts because:
        - Fewer HTTP requests (up to 25 items per request)
        - Lower latency overall
        - Better throughput
        
        Args:
            readings: List of dicts with device_id, timestamp, kwh
        
        Returns:
            int: Number of successfully written items
        
        Example:
            readings = [
                {"device_id": "device-001", "timestamp": "...", "kwh": 0.34},
                {"device_id": "device-001", "timestamp": "...", "kwh": 0.29}
            ]
            count = db.put_readings_batch(readings)
        """
        if not self.table:
            self.table = self.dynamodb.Table(self.table_name)
        
        success_count = 0
        
        # DynamoDB batch_write_item can handle max 25 items at a time
        # So we process readings in chunks of 25
        batch_size = 25
        
        for i in range(0, len(readings), batch_size):
            batch = readings[i:i + batch_size]
            
            try:
                # Use batch_writer context manager for automatic batching
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
        
        Uses Query operation which is efficient because:
        - Only reads items with matching partition key
        - Returns items in sorted order (by sort key)
        - Can be paginated for large datasets
        
        Args:
            device_id: The device/meter ID
        
        Returns:
            list: List of reading dictionaries
        
        Example:
            readings = db.get_readings_for_device("device-001")
            for r in readings:
                print(f"{r['timestamp']}: {r['kwh']} kWh")
        """
        if not self.table:
            self.table = self.dynamodb.Table(self.table_name)
        
        try:
            # Query for all items with this device_id
            # Key is imported from boto3.dynamodb.conditions
            response = self.table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('device_id').eq(device_id)
            )
            
            readings = []
            
            # Extract and convert items
            for item in response.get('Items', []):
                readings.append({
                    'device_id': item['device_id'],
                    'timestamp': item['timestamp'],
                    'kwh': float(item['kwh'])  # Convert Decimal back to float
                })
            
            # Handle pagination if there are more results
            # DynamoDB returns max 1MB of data per query
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
        Delete a specific reading from the table.
        
        In DynamoDB, you must specify the complete primary key
        (both partition key and sort key) to delete an item.
        
        Args:
            device_id: The device/meter ID (partition key)
            timestamp: The timestamp of the reading (sort key)
        
        Returns:
            bool: True if successful, False otherwise
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
        
        Uses Scan operation which reads the entire table.
        Note: Scan is expensive for large tables! Consider:
        - Using a Global Secondary Index (GSI)
        - Caching results
        - Limiting frequency of this operation
        
        Returns:
            list: List of unique device IDs
        
        Example:
            devices = db.get_all_devices()
            # Returns: ["device-001", "device-002", "device-003"]
        """
        if not self.table:
            self.table = self.dynamodb.Table(self.table_name)
        
        try:
            # Scan with projection to only return device_id attribute
            response = self.table.scan(
                ProjectionExpression='device_id'
            )
            
            # Use a set to automatically deduplicate
            devices = set()
            for item in response.get('Items', []):
                devices.add(item['device_id'])
            
            # Handle pagination for large tables
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
