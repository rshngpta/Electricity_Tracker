"""
=============================================================================
S3 SERVICE - Amazon Simple Storage Service Integration
=============================================================================
Student: Raushan Kumar
Course: Cloud Computing / AWS

What is S3?
-----------
Amazon S3 (Simple Storage Service) is an object storage service that offers:
- Unlimited storage capacity
- High durability (99.999999999% - 11 nines!)
- Pay only for what you use
- Access from anywhere via HTTP

In this application, we use S3 to:
- Store uploaded CSV files as backups
- Keep files safe and accessible from anywhere

Key S3 Concepts:
---------------
1. Bucket: A container for objects (like a folder in the cloud)
2. Object: A file stored in S3 (has a key/path and content)
3. Key: The unique identifier/path for an object in a bucket

Example:
    Bucket: electricity-tracker-uploads-24196517
    Key: uploads/20251128T120000Z_readings.csv
    Full path: s3://electricity-tracker-uploads-24196517/uploads/20251128T120000Z_readings.csv
=============================================================================
"""

# boto3 - The official AWS SDK (Software Development Kit) for Python
# It allows us to interact with all AWS services programmatically
import boto3

# ClientError - Exception class for AWS API errors
from botocore.exceptions import ClientError

# os - For reading environment variables
import os

# datetime - For generating timestamps
from datetime import datetime

# typing - For type hints (makes code more readable)
from typing import Optional, List, Dict


class S3Service:
    """
    A service class for interacting with Amazon S3.
    
    This class provides methods to:
    - Create S3 buckets
    - Upload files to S3
    - Download files from S3
    - List files in a bucket
    - Delete files from S3
    - Generate presigned URLs for temporary access
    
    Usage:
        s3 = S3Service()
        s3.create_bucket_if_not_exists()
        s3.upload_file(b"file content", "myfile.csv")
    """
    
    def __init__(self, bucket_name: str = None):
        """
        Initialize the S3 service.
        
        This constructor:
        1. Reads AWS credentials from environment variables
        2. Creates a boto3 S3 client
        
        AWS credentials are loaded from these environment variables:
        - AWS_ACCESS_KEY_ID: Your AWS access key
        - AWS_SECRET_ACCESS_KEY: Your AWS secret key
        - AWS_SESSION_TOKEN: Session token (for temporary credentials like Learner Lab)
        - AWS_REGION: The AWS region (e.g., 'us-east-1')
        
        Args:
            bucket_name: Optional custom bucket name. If not provided,
                        uses S3_BUCKET_NAME from environment or default.
        """
        # Get bucket name from parameter, environment variable, or use default
        self.bucket_name = bucket_name or os.getenv('S3_BUCKET_NAME', 'electricity-tracker-uploads')
        
        # Get AWS region from environment variable
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        
        # Check for session token (required for AWS Learner Lab temporary credentials)
        session_token = os.getenv('AWS_SESSION_TOKEN')
        
        # Create the S3 client
        # The client is the low-level interface to S3
        self.s3_client = boto3.client(
            's3',  # Service name
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            # Only include session_token if it exists (for Learner Lab)
            aws_session_token=session_token if session_token else None
        )
    
    def create_bucket_if_not_exists(self) -> bool:
        """
        Create the S3 bucket if it doesn't already exist.
        
        S3 bucket names must be:
        - Globally unique across ALL AWS accounts
        - Between 3-63 characters
        - Lowercase letters, numbers, and hyphens only
        - Start with a letter or number
        
        Returns:
            bool: True if bucket exists or was created successfully
        
        Note:
            Creating a bucket in us-east-1 uses different syntax than other regions.
        """
        try:
            # Check if bucket already exists by trying to get its metadata
            # head_bucket is a lightweight operation that just checks existence
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True  # Bucket exists
            
        except ClientError as e:
            # Get the error code from the response
            error_code = e.response['Error']['Code']
            
            if error_code == '404':
                # Bucket doesn't exist, let's create it
                try:
                    # us-east-1 is special - don't specify LocationConstraint
                    if self.region == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        # Other regions require LocationConstraint
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region}
                        )
                    print(f"Created bucket: {self.bucket_name}")
                    return True
                    
                except ClientError as create_error:
                    print(f"Failed to create bucket: {create_error}")
                    return False
            else:
                # Some other error (permissions, etc.)
                print(f"Error checking bucket: {e}")
                return False
    
    def upload_file(self, file_content: bytes, filename: str, content_type: str = 'text/csv') -> Optional[str]:
        """
        Upload a file to S3.
        
        The file is stored with a timestamp prefix to ensure uniqueness
        and organize files chronologically.
        
        Args:
            file_content: The file content as bytes
            filename: The original filename (e.g., "readings.csv")
            content_type: MIME type of the file (default: 'text/csv')
        
        Returns:
            str: The S3 key (path) of the uploaded file, or None if failed
        
        Example:
            key = s3.upload_file(b"device_id,timestamp,kwh\n...", "data.csv")
            # Returns: "uploads/20251128T120000Z_data.csv"
        """
        # Generate a unique key with timestamp prefix
        # Format: uploads/YYYYMMDDTHHMMSSZ_filename
        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        s3_key = f"uploads/{timestamp}_{filename}"
        
        try:
            # Upload the file using put_object
            self.s3_client.put_object(
                Bucket=self.bucket_name,  # Target bucket
                Key=s3_key,               # Path/name in the bucket
                Body=file_content,        # The actual file content
                ContentType=content_type  # MIME type for proper handling
            )
            return s3_key  # Return the key so caller knows where it's stored
            
        except ClientError as e:
            print(f"Failed to upload to S3: {e}")
            return None
    
    def download_file(self, s3_key: str) -> Optional[bytes]:
        """
        Download a file from S3.
        
        Args:
            s3_key: The S3 key (path) of the file to download
        
        Returns:
            bytes: The file content, or None if failed
        
        Example:
            content = s3.download_file("uploads/20251128T120000Z_data.csv")
            text = content.decode('utf-8')
        """
        try:
            # Get the object from S3
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            # Read and return the body content
            return response['Body'].read()
            
        except ClientError as e:
            print(f"Failed to download from S3: {e}")
            return None
    
    def list_files(self, prefix: str = 'uploads/') -> List[Dict]:
        """
        List all files in the S3 bucket.
        
        Args:
            prefix: Filter files by prefix/folder (default: 'uploads/')
        
        Returns:
            list: List of dictionaries with file metadata:
                - key: The file path/name
                - size: File size in bytes
                - last_modified: When the file was uploaded
        
        Example:
            files = s3.list_files()
            for f in files:
                print(f"{f['key']}: {f['size']} bytes")
        """
        try:
            # List objects in the bucket with the given prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            files = []
            # Extract relevant metadata for each object
            for obj in response.get('Contents', []):
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat()
                })
            return files
            
        except ClientError as e:
            print(f"Failed to list files: {e}")
            return []
    
    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3.
        
        Args:
            s3_key: The S3 key (path) of the file to delete
        
        Returns:
            bool: True if successful, False otherwise
        
        Warning:
            This action is irreversible! The file will be permanently deleted.
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            return True
            
        except ClientError as e:
            print(f"Failed to delete from S3: {e}")
            return False
    
    def get_presigned_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate a presigned URL for temporary file access.
        
        Presigned URLs allow temporary access to private S3 objects
        without requiring AWS credentials. Useful for:
        - Sharing files temporarily
        - Direct browser downloads
        - Secure file access without exposing credentials
        
        Args:
            s3_key: The S3 key (path) of the file
            expiration: URL validity in seconds (default: 3600 = 1 hour)
        
        Returns:
            str: The presigned URL, or None if failed
        
        Example:
            url = s3.get_presigned_url("uploads/data.csv", expiration=7200)
            # User can download the file using this URL for 2 hours
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',  # The S3 operation (download)
                Params={
                    'Bucket': self.bucket_name,
                    'Key': s3_key
                },
                ExpiresIn=expiration  # URL expiration time
            )
            return url
            
        except ClientError as e:
            print(f"Failed to generate presigned URL: {e}")
            return None
