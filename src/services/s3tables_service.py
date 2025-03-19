import boto3
from typing import Dict, Any, Optional, List
from ..utils.logging_utils import get_logger
from ..utils.aws_utils import create_s3tables_client
import traceback

logger = get_logger(__name__)

class S3TablesService:
    def __init__(self, config):
        self.config = config
        self.client = create_s3tables_client(
            region=self.config.aws_region,
            credentials={
                'aws_access_key_id': self.config.aws_access_key_id,
                'aws_secret_access_key': self.config.aws_secret_access_key
            }
        )

    def configure_table_maintenance(self, table_name: str):
        """Configure maintenance settings for a table"""
        try:
            if not self.config.maintenance_enabled:
                logger.logjson("INFO", f"Maintenance disabled for table: {table_name}")
                return

            maintenance_config = {
                'TableName': table_name,
                'NamespaceName': self.config.table_namespace,
                'TableBucketName': self.config.table_bucket,
                'Configuration': {
                    'CompactionEnabled': self.config.compaction_enabled,
                    'SnapshotRetentionPeriod': {
                        'Period': self.config.snapshot_retention_days,
                        'Unit': 'DAYS'
                    }
                }
            }

            self.client.put_table_maintenance_configuration(**maintenance_config)
            logger.logjson("INFO", f"Configured maintenance for table: {table_name}", {
                "config": maintenance_config
            })
        except Exception as e:
            logger.logjson("ERROR", f"Failed to configure maintenance for table {table_name}: {str(e)}")
            raise

    def setup_analytics_integration(self):
        """Verify and setup analytics service integration"""
        try:
            if not self.config.analytics_integration_enabled:
                logger.logjson("INFO", "Analytics integration disabled")
                return

            # Configure AWS Glue Data Catalog integration
            glue_config = {
                'TableBucketName': self.config.table_bucket,
                'GlueCatalogId': self.config.glue_catalog_id,
                'SageMakerIntegration': self.config.sagemaker_integration
            }

            self.client.put_table_bucket_analytics_configuration(**glue_config)

            logger.logjson("INFO", "Analytics integration configured", {
                "glue_catalog_id": self.config.glue_catalog_id,
                "sagemaker_enabled": self.config.sagemaker_integration
            })
        except Exception as e:
            logger.logjson("ERROR", f"Failed to configure analytics integration: {str(e)}")
            raise

    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        try:
            # Get table schema using S3 Tables API
            response = self.client.get_table(
                TableName=table_name,
                NamespaceName=self.config.table_namespace,
                TableBucketName=self.config.table_bucket
            )
            
            schema = {
                'columns': response['Table']['Columns'],
                'format': response['Table']['TableFormat'],
                'properties': response.get('Table', {}).get('Properties', {})
            }
            
            logger.logjson("INFO", f"Retrieved schema for table: {table_name}")
            return schema
        except Exception as e:
            logger.logjson("ERROR", f"Error getting table schema: {str(e)}\n{traceback.format_exc()}")
            return None

    def list_tables(self) -> Optional[List[str]]:
        try:
            # List all tables in the namespace using S3 Tables API
            paginator = self.client.get_paginator('list_tables')
            tables = []
            
            for page in paginator.paginate(
                NamespaceName=self.config.table_namespace,
                TableBucketName=self.config.table_bucket
            ):
                tables.extend([table['Name'] for table in page['Tables']])
            
            logger.logjson("INFO", f"Retrieved {len(tables)} tables from S3 Tables")
            return tables
        except Exception as e:
            logger.logjson("ERROR", f"Error listing tables: {str(e)}\n{traceback.format_exc()}")
            return None 