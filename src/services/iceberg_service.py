import boto3
import os
from typing import List, Dict, Any, Optional
from src.utils.logging_utils import get_logger
from src.utils.aws_utils import create_s3tables_client
from pyiceberg.catalog import load_catalog
import pandas as pd
import re
from src.services.database_service import DatabaseService

logger = get_logger(__name__)

class IcebergService:
    """Service for interacting with Iceberg tables"""
    
    def __init__(self, region: str, credentials: Dict[str, str], config: Any):
        """Initialize the Iceberg service"""
        self.region = region
        self.credentials = credentials  # Store credentials securely
        self.session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=region
        )
        logger.info(f"Initialized Iceberg service for region {region}")
        self.client = create_s3tables_client(region, credentials)
        
        # Get table bucket ARN from environment
        table_bucket_arn = os.environ.get('TABLE_BUCKET_ARN')
        if not table_bucket_arn:
            raise ValueError("TABLE_BUCKET_ARN environment variable is required")
        
        # Initialize PyIceberg catalog with S3 Tables REST endpoint configuration
        self.catalog = load_catalog(
            's3tables',
            **{
                'type': 'rest',
                'warehouse': table_bucket_arn,
                'uri': f"https://s3tables.{region}.amazonaws.com/iceberg",
                'rest.sigv4-enabled': 'true',
                'rest.signing-name': 's3tables',
                'rest.signing-region': region,
                'io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
                'aws.region': region,
                'aws.access-key-id': credentials['aws_access_key_id'],
                'aws.secret-access-key': credentials['aws_secret_access_key']
            }
        )
        
        # Initialize DatabaseService for query execution
        self.db_service = DatabaseService(config)
        self.db_service.connect()
        
    def _extract_table_info(self, query: str) -> tuple[str, str]:
        """Extract namespace and table name from an Iceberg query.
        
        Args:
            query: SQL query string
            
        Returns:
            tuple: (namespace, table_name)
        """
        # Match pattern: FROM iceberg.namespace.table
        pattern = r'FROM\s+iceberg\.([^.]+)\.([^\s]+)'
        match = re.search(pattern, query, re.IGNORECASE)
        
        if not match:
            # Try alternative pattern without 'iceberg.' prefix
            pattern = r'FROM\s+([^.]+)\.([^\s]+)'
            match = re.search(pattern, query, re.IGNORECASE)
            
            if not match:
                # If no namespace found, use 'default' namespace
                pattern = r'FROM\s+([^\s]+)'
                match = re.search(pattern, query, re.IGNORECASE)
                if not match:
                    raise ValueError("Query must contain a valid table reference (e.g., FROM table)")
                return "default", match.group(1)
            
        namespace, table_name = match.groups()
        return namespace, table_name
        
    async def execute_query(
        self,
        query: str,
        table_bucket_arn: Optional[str] = None,
        namespace: Optional[str] = None,
        table_name: Optional[str] = None,
        recursive: bool = False,
        columns: Optional[List[str]] = None,
        union_by_name: bool = False,
        s3_region: Optional[str] = None,
        s3_access_key_id: Optional[str] = None,
        s3_secret_access_key: Optional[str] = None,
        s3_session_token: Optional[str] = None,
        s3_endpoint: Optional[str] = None,
        s3_url_style: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute a query against an Iceberg table using DatabaseService.

        Args:
            query: SQL query string
            table_bucket_arn: Optional S3 bucket ARN for the table
            namespace: Optional namespace (extracted from query if not provided)
            table_name: Optional table name (extracted from query if not provided)
            recursive: Whether to recursively scan the table
            columns: Optional list of columns to select
            union_by_name: Whether to union tables by column name
            s3_region: Optional AWS region for S3
            s3_access_key_id: Optional AWS access key ID
            s3_secret_access_key: Optional AWS secret access key
            s3_session_token: Optional AWS session token
            s3_endpoint: Optional S3 endpoint URL
            s3_url_style: Optional S3 URL style

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries containing the query results.
        """
        # Extract table info from query if not provided
        if not namespace or not table_name:
            namespace, table_name = self._extract_table_info(query)
            
        # Use environment variable if table_bucket_arn not provided
        if not table_bucket_arn:
            table_bucket_arn = os.environ.get('TABLE_BUCKET_ARN')
            if not table_bucket_arn:
                raise ValueError("TABLE_BUCKET_ARN must be provided or set in environment")

        # Extract columns from query if not provided
        if not columns:
            # Try to extract columns from SELECT clause
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE)
            if select_match:
                columns = [col.strip() for col in select_match.group(1).split(',')]
            else:
                columns = None  # Will use SELECT * in read_s3_table

        # Read the table using read_s3_table
        df = self.db_service.read_s3_table(
            table_bucket_arn=table_bucket_arn,
            namespace=namespace,
            table_name=table_name,
            columns=columns,
            s3_region=s3_region or self.region,
            s3_access_key_id=s3_access_key_id or self.credentials.get('aws_access_key_id'),
            s3_secret_access_key=s3_secret_access_key or self.credentials.get('aws_secret_access_key'),
            s3_session_token=s3_session_token,
            s3_endpoint=s3_endpoint,
            s3_url_style=s3_url_style
        )

        # Apply any additional query conditions
        if query.lower().count('where') > 0:
            where_clause = re.search(r'WHERE\s+(.*?)(?:\s+ORDER BY|\s+GROUP BY|\s+LIMIT|$)', query, re.IGNORECASE)
            if where_clause:
                condition = where_clause.group(1)
                df = df.query(condition)

        # Apply ORDER BY if present
        if 'order by' in query.lower():
            order_match = re.search(r'ORDER BY\s+(.*?)(?:\s+GROUP BY|\s+LIMIT|$)', query, re.IGNORECASE)
            if order_match:
                order_clause = order_match.group(1)
                df = df.sort_values(by=[col.strip() for col in order_clause.split(',')])

        # Apply LIMIT if present
        limit_match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        if limit_match:
            limit = int(limit_match.group(1))
            df = df.head(limit)

        # Convert DataFrame to list of dictionaries
        return df.to_dict('records')
            
    async def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for an Iceberg table"""
        try:
            stats = self.db_service.get_table_stats(table_name)
            if stats is None:
                raise RuntimeError(f"Failed to get stats for table {table_name}")
            return stats
        except Exception as e:
            logger.error(f"Error getting Iceberg table stats: {str(e)}")
            raise
            
    async def get_tables(self) -> List[Dict[str, Any]]:
        """List all Iceberg tables in the catalog."""
        try:
            tables = self.catalog.list_tables('default')
            bucket_name = os.environ.get('TABLE_BUCKET_NAME', 'default-bucket')
            return [
                {
                    "name": table[1],
                    "location": f"s3://{bucket_name}/{table[1]}",
                    "format": "iceberg"
                }
                for table in tables
            ]
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}")
            raise

    def __del__(self):
        """Cleanup when the service is destroyed"""
        if hasattr(self, 'db_service'):
            self.db_service.close()