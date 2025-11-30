"""
=============================================================================
LAMBDA SERVICE - AWS Lambda (Serverless Functions) Integration
=============================================================================
Student: Raushan Kumar
Course: Cloud Computing / AWS

What is AWS Lambda?
-------------------
AWS Lambda is a serverless compute service that:
- Runs your code without managing servers
- Scales automatically (from 0 to thousands of instances)
- Charges only for compute time used (pay per millisecond)
- Supports multiple languages: Python, Node.js, Java, Go, etc.

Key Benefits:
- No server management
- Automatic scaling
- Pay only for what you use
- Event-driven architecture

Lambda Concepts:
---------------
1. Function: Your code packaged and deployed to Lambda
2. Handler: The entry point function (e.g., lambda_handler)
3. Event: The trigger/input that invokes the function
4. Context: Runtime information passed to the function
5. Invocation Types:
   - Synchronous (RequestResponse): Wait for result
   - Asynchronous (Event): Fire and forget

Lambda Triggers:
---------------
Lambda functions can be triggered by:
- API Gateway (HTTP requests)
- S3 (file uploads)
- DynamoDB Streams (data changes)
- CloudWatch Events (scheduled)
- SNS/SQS (messages)
- And many more...

Our Lambda Functions:
--------------------
1. electricity-get-usage: Get usage data (via API Gateway)
2. process_upload: Process CSV uploads (via S3 trigger)
3. estimate_bill: Calculate bill estimates (via API Gateway)
4. send_alert: Send notifications (via API/DynamoDB Stream)

Example Lambda Handler:
----------------------
def lambda_handler(event, context):
    # event = input data (e.g., API request body)
    # context = runtime info (function name, memory, timeout)
    
    device_id = event['queryStringParameters']['device_id']
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Success'})
    }
=============================================================================
"""

# boto3 - AWS SDK for Python
import boto3

# ClientError - Exception class for AWS API errors
from botocore.exceptions import ClientError

# os - For reading environment variables
import os

# json - For serializing/deserializing payloads
import json

# typing - For type hints
from typing import Optional, Dict, Any


class LambdaService:
    """
    A service class for interacting with AWS Lambda.
    
    This class provides methods to:
    - Invoke Lambda functions
    - List available functions
    - Check function existence
    
    Usage:
        lambda_svc = LambdaService()
        result = lambda_svc.invoke_function(
            "electricity-get-usage",
            {"device_id": "device-001"}
        )
    """
    
    def __init__(self):
        """
        Initialize the Lambda service.
        
        Creates a boto3 Lambda client using credentials
        from environment variables.
        """
        # Get AWS region
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        
        # Session token for Learner Lab
        session_token = os.getenv('AWS_SESSION_TOKEN')
        
        # Create Lambda client
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
        Invoke (call) a Lambda function.
        
        This method executes a Lambda function and optionally
        waits for the result.
        
        Args:
            function_name: Name or ARN of the Lambda function
                          Examples:
                          - "electricity-get-usage" (name only)
                          - "arn:aws:lambda:us-east-1:123456:function:my-func" (full ARN)
            
            payload: Dictionary to send as input to the function
                    This becomes the 'event' parameter in the handler
            
            invocation_type: How to invoke the function
                - 'RequestResponse': Synchronous - wait for result (default)
                - 'Event': Asynchronous - don't wait, fire and forget
                - 'DryRun': Validate without executing
        
        Returns:
            dict: The function's response (for RequestResponse), or
                  status info (for Event/DryRun), or None if failed
        
        Example:
            # Synchronous invocation (wait for result)
            result = lambda_svc.invoke_function(
                "electricity-get-usage",
                {
                    "queryStringParameters": {
                        "device_id": "device-001",
                        "period": "day"
                    }
                }
            )
            print(result)  # {'statusCode': 200, 'body': '...'}
            
            # Asynchronous invocation (fire and forget)
            lambda_svc.invoke_function(
                "send-alert",
                {"device_id": "device-001"},
                invocation_type='Event'
            )
        """
        try:
            # Invoke the Lambda function
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,
                # Payload must be bytes, so we JSON serialize
                Payload=json.dumps(payload)
            )
            
            if invocation_type == 'RequestResponse':
                # For synchronous calls, read and parse the response
                # The Payload is a StreamingBody object
                result = json.loads(response['Payload'].read().decode('utf-8'))
                return result
            else:
                # For async calls, just return status info
                return {
                    "status": "invoked",
                    "StatusCode": response['StatusCode']
                }
                
        except ClientError as e:
            print(f"Failed to invoke Lambda: {e}")
            return None
    
    def list_functions(self) -> list:
        """
        List all Lambda functions in the AWS account.
        
        Returns basic information about each function including:
        - FunctionName: The function name
        - FunctionArn: The full ARN
        - Runtime: e.g., python3.11, nodejs18.x
        - Handler: Entry point (e.g., lambda_function.lambda_handler)
        - CodeSize: Size in bytes
        - LastModified: When last updated
        
        Returns:
            list: List of function dictionaries
        
        Example:
            functions = lambda_svc.list_functions()
            for func in functions:
                print(f"{func['FunctionName']}: {func['Runtime']}")
        """
        try:
            response = self.lambda_client.list_functions()
            return response.get('Functions', [])
            
        except ClientError as e:
            print(f"Failed to list functions: {e}")
            return []
    
    def get_function(self, function_name: str) -> Optional[Dict]:
        """
        Get detailed information about a specific Lambda function.
        
        Returns comprehensive details including:
        - Configuration: Runtime, memory, timeout, environment variables
        - Code: Location, size, SHA256 hash
        - Tags: Custom metadata tags
        
        Args:
            function_name: Name or ARN of the function
        
        Returns:
            dict: Function details, or None if not found
        """
        try:
            response = self.lambda_client.get_function(
                FunctionName=function_name
            )
            return response
            
        except ClientError as e:
            print(f"Failed to get function: {e}")
            return None
    
    def function_exists(self, function_name: str) -> bool:
        """
        Check if a Lambda function exists.
        
        Useful for validation before invoking a function.
        
        Args:
            function_name: Name or ARN of the function
        
        Returns:
            bool: True if function exists, False otherwise
        
        Example:
            if lambda_svc.function_exists("electricity-get-usage"):
                result = lambda_svc.invoke_function("electricity-get-usage", {})
            else:
                print("Function not deployed yet!")
        """
        return self.get_function(function_name) is not None
