import os
from typing import Dict, Any
from src.utils.logging_utils import get_logger
from src.utils.aws_utils import get_aws_credentials

logger = get_logger(__name__)

class FederatedQueryConfig:
    """Configuration for the federated query layer"""
    
    def __init__(self):
        """Initialize configuration from environment variables"""
        # AWS Configuration
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.aws_secret_name = os.getenv('AWS_SECRET_NAME', 'local-dev')
        self.s3_table_bucket = os.getenv('S3_TABLE_BUCKET', 'local-dev-bucket')
        
        # Application Settings
        self.query_timeout = int(os.getenv('QUERY_TIMEOUT', '300'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_delay = int(os.getenv('RETRY_DELAY', '5'))
        self.metrics_interval = int(os.getenv('METRICS_INTERVAL', '60'))
        self.metrics_retention_seconds = int(os.getenv('METRICS_RETENTION_SECONDS', '3600'))
        
        # PostgreSQL Configuration
        self.postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.postgres_port = int(os.getenv('POSTGRES_PORT', '5432'))
        self.postgres_database = os.getenv('POSTGRES_DATABASE', 'your_database')
        self.postgres_user = os.getenv('POSTGRES_USER', 'your_username')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', 'your_password')
        self.postgres_ssl_mode = os.getenv('POSTGRES_SSL_MODE', 'prefer')
        self.postgres_pool_size = int(os.getenv('POSTGRES_POOL_SIZE', '5'))
        
        # API Settings
        self.port = int(os.getenv('PORT', '8000'))
        self.host = os.getenv('HOST', '0.0.0.0')
        
        # Iceberg Configuration
        self.iceberg_catalog = os.getenv('ICEBERG_CATALOG', 'local-dev-catalog')
        self.iceberg_namespace = os.getenv('ICEBERG_NAMESPACE', 'default')
        
        # Table Maintenance Configuration
        self.maintenance_enabled = os.getenv('MAINTENANCE_ENABLED', 'true').lower() == 'true'
        self.compaction_enabled = os.getenv('COMPACTION_ENABLED', 'true').lower() == 'true'
        self.snapshot_retention_days = int(os.getenv('SNAPSHOT_RETENTION_DAYS', '30'))
        
        # Analytics Integration Configuration
        self.analytics_integration_enabled = os.getenv('ANALYTICS_INTEGRATION_ENABLED', 'false').lower() == 'true'
        self.glue_catalog_id = os.getenv('GLUE_CATALOG_ID', '')
        self.sagemaker_integration = os.getenv('SAGEMAKER_INTEGRATION', 'false').lower() == 'true'
        
        # Get AWS credentials
        credentials = get_aws_credentials(self.aws_region, self.aws_secret_name)
        self.aws_access_key_id = credentials.get('aws_access_key_id')
        self.aws_secret_access_key = credentials.get('aws_secret_access_key')
        
    def validate(self) -> bool:
        """Validate the configuration"""
        # For local development, we don't require AWS credentials
        if os.getenv('ENVIRONMENT') == 'production':
            required_vars = [
                'AWS_REGION',
                'AWS_SECRET_NAME',
                'S3_TABLE_BUCKET',
                'POSTGRES_HOST',
                'POSTGRES_DATABASE',
                'POSTGRES_USER',
                'POSTGRES_PASSWORD',
                'ICEBERG_CATALOG'
            ]
            
            missing_vars = [var for var in required_vars if not os.getenv(var)]
            if missing_vars:
                logger.logjson("ERROR", f"Missing required environment variables: {missing_vars}")
                return False
                
            if not self.aws_access_key_id or not self.aws_secret_access_key:
                logger.logjson("ERROR", "Missing AWS credentials")
                return False
                
        return True 