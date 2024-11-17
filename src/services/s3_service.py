import boto3
import logging
from typing import Optional
from src.config import Config

logger = logging.getLogger(__name__)

class S3Service:
    def __init__(self):
        self.config = Config()
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.config.AWS_SECRET_ACCESS_KEY,
            region_name=self.config.AWS_DEFAULT_REGION
        )
        
    def get_image(self, image_url: str) -> Optional[bytes]:
        try:
            bucket = self.config.AWS_BUCKET_NAME
            key = image_url.split(f'{bucket}.s3.amazonaws.com/')[-1]
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
        except Exception as e:
            logger.error(f"S3 error: {e}")
            return None
