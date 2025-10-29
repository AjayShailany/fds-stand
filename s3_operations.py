
import boto3
from botocore.exceptions import ClientError
import logging
import os
from config import AWS_S3_BUCKET

logger = logging.getLogger(__name__)

class S3Operations:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket = AWS_S3_BUCKET
        self.prefix = 'FDA_STANDARDS/'

    def upload_file(self, local_path: str, s3_key: str, content_type: str) -> bool:
        """Upload a file to S3."""
        try:
            self.s3_client.upload_file(
                local_path,
                self.bucket,
                s3_key,
                ExtraArgs={'ContentType': content_type}
            )
            logger.info(f"Uploaded {local_path} to s3://{self.bucket}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_path} to S3: {str(e)}")
            return False

    def file_exists(self, s3_key: str) -> bool:
        """Check if a file exists in S3."""
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
                return False
            logger.error(f"Error checking S3 file {s3_key}: {str(e)}")
            return False