# backend/lib/lambda_service.py
"""
Lambda Service for Electricity Tracker
Handles invoking and managing AWS Lambda functions
"""
import boto3
from botocore.exceptions import ClientError
import os
import json
from typing import Optional, Dict, Any


class LambdaService:
    def __init__(self):
        """
        Initialize Lambda service.
        
        Uses AWS credentials from environment variables.
        """
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        session_token = os.getenv('AWS_SESSION_TOKEN')
        
        self.lambda_client = boto3.client(
            'lambda',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=session_token if session_token else None
        )
    
    def invoke_function(self, function_name: str, payload: Dict[str, Any], 
                        invocation_type: str = 'RequestResponse') -> Optional[Dict]:
        """
        Invoke a Lambda function.
        
        Args:
            function_name: Name or ARN of the Lambda function
            payload: JSON payload to send to the function
            invocation_type: 'RequestResponse' (sync) or 'Event' (async)
            
        Returns:
            Response from the Lambda function, or None if failed
        """
        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,
                Payload=json.dumps(payload)
            )
            
            if invocation_type == 'RequestResponse':
                result = json.loads(response['Payload'].read().decode('utf-8'))
                return result
            else:
                return {"status": "invoked", "StatusCode": response['StatusCode']}
                
        except ClientError as e:
            print(f"Failed to invoke Lambda: {e}")
            return None
    
    def list_functions(self) -> list:
        """List all Lambda functions in the account."""
        try:
            response = self.lambda_client.list_functions()
            return response.get('Functions', [])
        except ClientError as e:
            print(f"Failed to list functions: {e}")
            return []
    
    def get_function(self, function_name: str) -> Optional[Dict]:
        """Get details of a specific Lambda function."""
        try:
            response = self.lambda_client.get_function(FunctionName=function_name)
            return response
        except ClientError as e:
            print(f"Failed to get function: {e}")
            return None
    
    def function_exists(self, function_name: str) -> bool:
        """Check if a Lambda function exists."""
        return self.get_function(function_name) is not None

