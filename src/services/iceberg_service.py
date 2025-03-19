import boto3
import os
from typing import List, Dict, Any, Optional
from src.utils.logging_utils import get_logger
from src.utils.aws_utils import create_s3tables_client
from pyiceberg.catalog import load_catalog
import pandas as pd
import re
import duckdb
import time
from datetime import datetime
from ..utils.query_utils import build_select_query, build_stats_query, build_distinct_values_query
from ..utils.error_utils import with_retry, handle_database_error, CircuitBreaker
import traceback
import hashlib
from functools import lru_cache
import json
import subprocess

logger = get_logger(__name__)

class IcebergService:
    """Service for interacting with Iceberg tables"""
    
    def __init__(self, region: str, credentials: Dict[str, str], config: Any):
        """Initialize the Iceberg service"""
        self.region = region
        self.credentials = credentials
        self.config = config
        self.session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=region
        )
        logger.info(f"Initialized Iceberg service for region {region}")
        self.client = create_s3tables_client(region, credentials)
        
        # Get table bucket ARN from environment
        self.table_bucket_arn = os.environ.get('TABLE_BUCKET_ARN')
        if not self.table_bucket_arn:
            raise ValueError("TABLE_BUCKET_ARN environment variable is required")
        
        # Initialize PyIceberg catalog with S3 Tables REST endpoint configuration
        self.catalog = load_catalog(
            's3tables',
            **{
                'type': 'rest',
                'warehouse': self.table_bucket_arn,
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
        
        # Initialize DuckDB connection
        self.connection = None
        self._query_cache = {}
        self._table_cache = {}
        self._cache_ttl = 300  # 5 minutes cache TTL
        self._table_cache_metadata = {}
        self._circuit_breaker = CircuitBreaker(
            threshold=5,
            reset_timeout=60.0
        )
        
        # Connect to DuckDB
        self.connect()

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

    @with_retry(max_attempts=3, base_delay=1.0, max_delay=10.0)
    @handle_database_error
    def connect(self):
        """Initialize DuckDB connection and load required extensions"""
        try:
            self.connection = duckdb.connect()
            self.connection.execute("INSTALL aws;")
            self.connection.execute("INSTALL httpfs;")
            self.connection.execute("INSTALL iceberg;")
            self.connection.execute("LOAD aws;")
            self.connection.execute("LOAD httpfs;")
            self.connection.execute("LOAD iceberg;")
            logger.logjson("INFO", "Successfully connected to DuckDB and loaded extensions")
        except Exception as e:
            logger.logjson("ERROR", f"Failed to connect to DuckDB: {str(e)}\n{traceback.format_exc()}")
            raise

    def _get_query_hash(self, query: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Generate a hash for the query and its parameters"""
        query_data = {
            'query': query,
            'params': params or {}
        }
        return hashlib.md5(json.dumps(query_data, sort_keys=True).encode()).hexdigest()

    def _is_cache_valid(self, cache_entry: Dict[str, Any]) -> bool:
        """Check if a cache entry is still valid"""
        return (time.time() - cache_entry['timestamp']) < self._cache_ttl

    def _get_table_cache_key(self, table_bucket_arn: str, namespace: str, table_name: str) -> str:
        """Generate a cache key for a table"""
        return f"{table_bucket_arn}:{namespace}:{table_name}"

    def _is_table_cache_valid(self, cache_key: str, max_age_seconds: int = 60) -> bool:
        """Check if a table cache is still valid based on time"""
        if cache_key not in self._table_cache_metadata:
            return False
        metadata = self._table_cache_metadata[cache_key]
        age = time.time() - metadata['timestamp']
        return age < max_age_seconds

    def _optimize_query(self, query: str) -> str:
        """Optimize the query for better performance"""
        if (
            "limit" not in query.lower() and
            "group by" not in query.lower() and
            "count(" not in query.lower() and
            "sum(" not in query.lower() and
            "avg(" not in query.lower()
        ):
            query = f"{query.rstrip(';')} LIMIT 1000;"
        return query

    def read_s3_table(
        self,
        table_bucket_arn: str,
        namespace: str,
        table_name: str,
        recursive: bool = False,
        columns: Optional[List[str]] = None,
        union_by_name: bool = False,
        s3_region: Optional[str] = None,
        s3_access_key_id: Optional[str] = None,
        s3_secret_access_key: Optional[str] = None,
        s3_session_token: Optional[str] = None,
        s3_endpoint: Optional[str] = None,
        s3_url_style: Optional[str] = None,
        use_cache: bool = True,
        max_cache_age: int = 60,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """Read an Iceberg table stored in AWS S3 using DuckDB"""
        cache_key = self._get_table_cache_key(table_bucket_arn, namespace, table_name)
        
        if use_cache and not force_refresh:
            if cache_key in self._table_cache and self._is_table_cache_valid(cache_key, max_cache_age):
                logger.debug(f"Cache hit for table {table_name}")
                return self._table_cache[cache_key]
            elif cache_key in self._table_cache:
                logger.debug(f"Cache expired for table {table_name}")

        # Get table metadata from AWS CLI
        metadata_cmd = [
            "aws", "s3tables", "get-table",
            "--table-bucket-arn", table_bucket_arn,
            "--namespace", namespace,
            "--name", table_name,
            "--region", s3_region or self.region
        ]

        try:
            metadata_output = subprocess.check_output(metadata_cmd, universal_newlines=True)
            metadata_json = json.loads(metadata_output)
            metadata_location = metadata_json.get("metadataLocation")

            if not metadata_location:
                raise ValueError("Error: metadataLocation not found in AWS response.")

        except Exception as e:
            logger.error(f"Error retrieving table metadata: {str(e)}")
            raise RuntimeError(f"Error retrieving table metadata: {str(e)}")

        # Configure AWS S3 Credentials for DuckDB
        secret_params = []
        if s3_region:
            secret_params.append(f"REGION '{s3_region}'")
        if s3_access_key_id:
            secret_params.append(f"KEY_ID '{s3_access_key_id}'")
        if s3_secret_access_key:
            secret_params.append(f"SECRET '{s3_secret_access_key}'")
        if s3_session_token:
            secret_params.append(f"SESSION_TOKEN '{s3_session_token}'")
        if s3_endpoint:
            secret_params.append(f"ENDPOINT '{s3_endpoint}'")
        if s3_url_style:
            secret_params.append(f"URL_STYLE '{s3_url_style}'")

        if secret_params:
            secret_sql = f"CREATE OR REPLACE TEMPORARY SECRET s3_secret (TYPE S3, {', '.join(secret_params)})"
            self.connection.execute(secret_sql)

        # Query the Iceberg Table
        query = f"SELECT * FROM iceberg_scan('{metadata_location}')"
        if columns:
            query = f"SELECT {', '.join(columns)} FROM iceberg_scan('{metadata_location}')"

        logger.debug(f"Running query: {query}")
        result = self.connection.execute(query).df()
        
        # Cache the result if enabled
        if use_cache:
            self._table_cache[cache_key] = result
            self._table_cache_metadata[cache_key] = {
                'timestamp': time.time(),
                'row_count': len(result),
                'columns': result.columns.tolist()
            }
            logger.debug(f"Cached table {table_name} with {len(result)} rows")
        
        return result

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
        """Execute a query against an Iceberg table"""
        # Extract table info from query if not provided
        if not namespace or not table_name:
            namespace, table_name = self._extract_table_info(query)
            
        # Use environment variable if table_bucket_arn not provided
        if not table_bucket_arn:
            table_bucket_arn = self.table_bucket_arn

        # Extract columns from query if not provided
        if not columns:
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE)
            if select_match:
                columns = [col.strip() for col in select_match.group(1).split(',')]
            else:
                columns = None

        # Read the table
        df = self.read_s3_table(
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

        # Apply query conditions
        if query.lower().count('where') > 0:
            where_clause = re.search(r'WHERE\s+(.*?)(?:\s+ORDER BY|\s+GROUP BY|\s+LIMIT|$)', query, re.IGNORECASE)
            if where_clause:
                condition = where_clause.group(1)
                df = df.query(condition)

        # Apply ORDER BY
        if 'order by' in query.lower():
            order_match = re.search(r'ORDER BY\s+(.*?)(?:\s+GROUP BY|\s+LIMIT|$)', query, re.IGNORECASE)
            if order_match:
                order_clause = order_match.group(1)
                df = df.sort_values(by=[col.strip() for col in order_clause.split(',')])

        # Apply LIMIT
        limit_match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        if limit_match:
            limit = int(limit_match.group(1))
            df = df.head(limit)

        return df.to_dict('records')

    async def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for an Iceberg table"""
        try:
            stats = {}
            
            # Get total row count
            count_query = f"SELECT COUNT(*) as total_rows FROM s3tables.{table_name};"
            result = self.execute_query(count_query)
            stats['total_rows'] = result[0]['total_rows'] if result else 0
            
            # Get schema
            schema = self.get_table_schema(table_name)
            if schema:
                stats['schema'] = schema
            
            # Get column statistics for numeric columns
            if schema and 'columns' in schema:
                column_stats = {}
                for col in schema['columns']:
                    if col.get('Type', '').lower() in ['double', 'float', 'int', 'bigint', 'decimal']:
                        stats_query = build_stats_query(table_name, col['Name'])
                        col_stats = self.execute_query(stats_query)
                        if col_stats:
                            column_stats[col['Name']] = col_stats[0]
                
                stats['column_stats'] = column_stats
            
            return stats
                
        except Exception as e:
            logger.error(f"Error getting Iceberg table stats: {str(e)}")
            raise

    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get schema information for a table"""
        try:
            query = f"""
                SELECT column_name as Name, data_type as Type
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position;
            """
            columns = self.execute_query(query)
            if columns:
                return {'columns': columns}
            return None
        except Exception as e:
            logger.logjson("ERROR", f"Error getting table schema for {table_name}: {str(e)}")
            return None

    async def get_tables(self) -> List[Dict[str, Any]]:
        """List all Iceberg tables in the catalog"""
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

    def clear_table_cache(self, table_name: Optional[str] = None):
        """Clear the table cache, optionally for a specific table"""
        if table_name:
            keys_to_remove = [k for k in self._table_cache.keys() if table_name in k]
            for k in keys_to_remove:
                del self._table_cache[k]
                if k in self._table_cache_metadata:
                    del self._table_cache_metadata[k]
            logger.debug(f"Cleared cache for table {table_name}")
        else:
            self._table_cache.clear()
            self._table_cache_metadata.clear()
            logger.debug("Cleared entire table cache")

    def configure_table_maintenance(self, table_name: str):
        """Configure maintenance settings for a table"""
        try:
            if not self.config.maintenance_enabled:
                logger.logjson("INFO", f"Maintenance disabled for table: {table_name}")
                return

            maintenance_config = {
                'TableName': table_name,
                'NamespaceName': self.config.iceberg_namespace,
                'TableBucketName': self.config.s3_table_bucket,
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
                'TableBucketName': self.config.s3_table_bucket,
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

    def __del__(self):
        """Cleanup when the service is destroyed"""
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                logger.logjson("ERROR", f"Error closing connection: {str(e)}")