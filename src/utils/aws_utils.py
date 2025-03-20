import boto3
import json
from botocore.config import Config
from typing import Dict, Any, Optional
from .logging_utils import get_logger

logger = get_logger(__name__)

def get_aws_credentials(region: str, secret_name: str) -> Dict[str, str]:
    """
    Get AWS credentials from Secrets Manager
    For local development, this is a placeholder that returns empty credentials
    """
    try:
        # TODO: Implement actual AWS Secrets Manager integration
        # For now, return empty credentials for local development
        return {
            'aws_access_key_id': '',
            'aws_secret_access_key': ''
        }
    except Exception as e:
        logger.logjson("ERROR", f"Error getting AWS credentials: {str(e)}")
        raise

def _get_secrets_from_manager(region: str, secret_name: str) -> Dict[str, str]:
    """Get secrets from AWS Secrets Manager"""
    try:
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region
        )
        secret_value = client.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value['SecretString'])
    except Exception as e:
        logger.logjson("ERROR", f"Failed to get secrets from AWS Secrets Manager: {str(e)}")
        return {}

def _get_secrets_from_env() -> Dict[str, str]:
    """Get AWS credentials from environment variables"""
    import os
    return {
        'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY')
    }

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