import boto3
from botocore.config import Config
from typing import Any
from .logging_utils import get_logger

logger = get_logger(__name__)

def create_s3tables_client(region: str) -> Any:
    """
    Create an S3 Tables client using the default credential provider chain
    """
    try:
        session = boto3.Session(region_name=region)
        
        # Create S3 Tables client with proper configuration
        config = Config(
            retries=dict(
                max_attempts=3,
                mode='standard'
            ),
            connect_timeout=60,
            read_timeout=60
        )
        
        client = session.client(
            's3tables',
            endpoint_url=f'https://s3tables.{region}.amazonaws.com',
            config=config
        )
        
        return client
    except Exception as e:
        logger.logjson("ERROR", f"Error creating S3 Tables client: {str(e)}")
        raise 