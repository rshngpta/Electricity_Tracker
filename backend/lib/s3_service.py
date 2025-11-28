# backend/lib/s3_service.py
"""
S3 Service for Electricity Tracker
Handles file uploads/downloads to AWS S3
"""
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime
from typing import Optional, List, Dict

class S3Service:
    def __init__(self, bucket_name: str = None):
        """
        Initialize S3 service.
        
        Uses AWS credentials from environment variables:
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY
        - AWS_REGION (defaults to us-east-1)
        """
        self.bucket_name = bucket_name or os.getenv('S3_BUCKET_NAME', 'electricity-tracker-uploads')
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        
        # Support session token for AWS Learner Lab
        session_token = os.getenv('AWS_SESSION_TOKEN')
        
        self.s3_client = boto3.client(
            's3',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=session_token if session_token else None
        )
    
    def create_bucket_if_not_exists(self) -> bool:
        """Create the S3 bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                try:
                    if self.region == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
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
                print(f"Error checking bucket: {e}")
                return False
    
    def upload_file(self, file_content: bytes, filename: str, content_type: str = 'text/csv') -> Optional[str]:
        """
        Upload a file to S3.
        
        Args:
            file_content: The file content as bytes
            filename: The original filename
            content_type: MIME type of the file
            
        Returns:
            The S3 key (path) of the uploaded file, or None if failed
        """
        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        s3_key = f"uploads/{timestamp}_{filename}"
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=file_content,
                ContentType=content_type
            )
            return s3_key
        except ClientError as e:
            print(f"Failed to upload to S3: {e}")
            return None
    
    def download_file(self, s3_key: str) -> Optional[bytes]:
        """
        Download a file from S3.
        
        Args:
            s3_key: The S3 key (path) of the file
            
        Returns:
            The file content as bytes, or None if failed
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body'].read()
        except ClientError as e:
            print(f"Failed to download from S3: {e}")
            return None
    
    def list_files(self, prefix: str = 'uploads/') -> List[Dict]:
        """
        List files in the S3 bucket.
        
        Args:
            prefix: Filter files by prefix (folder path)
            
        Returns:
            List of file metadata dictionaries
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            files = []
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
            s3_key: The S3 key (path) of the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            print(f"Failed to delete from S3: {e}")
            return False
    
    def get_presigned_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate a presigned URL for downloading a file.
        
        Args:
            s3_key: The S3 key (path) of the file
            expiration: URL expiration time in seconds (default 1 hour)
            
        Returns:
            The presigned URL, or None if failed
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': s3_key},
                ExpiresIn=expiration
            )
            return url
        except ClientError as e:
            print(f"Failed to generate presigned URL: {e}")
            return None

