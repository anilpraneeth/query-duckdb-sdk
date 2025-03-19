import duckdb
from typing import Dict, Any, Optional, List, Generator
import time
from datetime import datetime
from ..utils.logging_utils import get_logger
from ..utils.query_utils import build_select_query, build_stats_query, build_distinct_values_query
from ..utils.error_utils import with_retry, handle_database_error, CircuitBreaker
import traceback
import hashlib
from functools import lru_cache
import json
import subprocess
import pandas as pd

logger = get_logger(__name__)

class DatabaseService:
    def __init__(self, config):
        self.config = config
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self._query_cache = {}
        self._table_cache = {}  # Cache for table DataFrames
        self._cache_ttl = 300  # 5 minutes cache TTL
        self._table_cache_metadata = {}  # Store metadata about cached tables
        self._circuit_breaker = CircuitBreaker(
            threshold=5,
            reset_timeout=60.0
        )

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

    @with_retry(max_attempts=3, base_delay=1.0, max_delay=10.0)
    @handle_database_error
    def connect(self):
        try:
            # Initialize DuckDB connection
            self.connection = duckdb.connect()
            
            # Install required extensions
            self.connection.execute("INSTALL aws;")
            self.connection.execute("INSTALL httpfs;")
            self.connection.execute("INSTALL iceberg;")
            
            # Load extensions
            self.connection.execute("LOAD aws;")
            self.connection.execute("LOAD httpfs;")
            self.connection.execute("LOAD iceberg;")
            
            logger.logjson("INFO", "Successfully connected to DuckDB and loaded extensions")
        except Exception as e:
            logger.logjson("ERROR", f"Failed to connect to DuckDB: {str(e)}\n{traceback.format_exc()}")
            raise

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
        max_cache_age: int = 60,  # Maximum age of cached data in seconds
        force_refresh: bool = False,  # Force refresh the cache
    ) -> pd.DataFrame:
        """
        Read an Iceberg table stored in AWS S3 using DuckDB.

        Args:
            ... (existing args) ...
            max_cache_age: Maximum age of cached data in seconds (default: 60)
            force_refresh: Force refresh the cache regardless of age

        Returns
        -------
        pd.DataFrame
            A DataFrame representing the Iceberg table.
        """
        cache_key = self._get_table_cache_key(table_bucket_arn, namespace, table_name)
        
        # Check if we should use cache
        if use_cache and not force_refresh:
            if cache_key in self._table_cache and self._is_table_cache_valid(cache_key, max_cache_age):
                logger.debug(f"Cache hit for table {table_name} (age: {time.time() - self._table_cache_metadata[cache_key]['timestamp']:.1f}s)")
                return self._table_cache[cache_key]
            elif cache_key in self._table_cache:
                logger.debug(f"Cache expired for table {table_name}")

        # Step 1: Retrieve Table Metadata from AWS CLI
        metadata_cmd = [
            "aws", "s3tables", "get-table",
            "--table-bucket-arn", table_bucket_arn,
            "--namespace", namespace,
            "--name", table_name,
            "--region", s3_region or self.config.aws_region
        ]

        try:
            metadata_output = subprocess.check_output(metadata_cmd, universal_newlines=True)
            metadata_json = json.loads(metadata_output)

            # Debug: Print metadata response
            logger.debug(f"AWS Metadata Response: {json.dumps(metadata_json, indent=4)}")

            # Extract metadata location for querying
            metadata_location = metadata_json.get("metadataLocation")

            if not metadata_location:
                raise ValueError("Error: metadataLocation not found in AWS response.")

        except Exception as e:
            logger.error(f"Error retrieving table metadata: {str(e)}")
            raise RuntimeError(f"Error retrieving table metadata: {str(e)}")

        # Step 2: Configure AWS S3 Credentials for DuckDB
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

        # Step 3: Query the Iceberg Table using DuckDB
        query = f"SELECT * FROM iceberg_scan('{metadata_location}')"
        if columns:
            query = f"SELECT {', '.join(columns)} FROM iceberg_scan('{metadata_location}')"

        logger.debug(f"Running query: {query}")
        
        # Execute query and convert to pandas DataFrame
        result = self.connection.execute(query).df()
        logger.debug(f"Result schema: {result.columns.tolist()}")
        logger.debug(f"Result shape: {result.shape}")
        
        # Cache the result if caching is enabled
        if use_cache:
            self._table_cache[cache_key] = result
            self._table_cache_metadata[cache_key] = {
                'timestamp': time.time(),
                'row_count': len(result),
                'columns': result.columns.tolist()
            }
            logger.debug(f"Cached table {table_name} with {len(result)} rows")
        
        return result

    def _optimize_query(self, query: str) -> str:
        """Optimize the query for better performance"""
        # Add LIMIT if not present and query doesn't have aggregation
        if (
            "limit" not in query.lower() and
            "group by" not in query.lower() and
            "count(" not in query.lower() and
            "sum(" not in query.lower() and
            "avg(" not in query.lower()
        ):
            query = f"{query.rstrip(';')} LIMIT 1000;"
            
        return query

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Execute a query with caching and optimization"""
        @with_retry(max_attempts=3, base_delay=1.0, max_delay=10.0, circuit_breaker=self._circuit_breaker)
        @handle_database_error
        def _execute():
            try:
                start_time = time.time()
                query_hash = self._get_query_hash(query, params)
                
                # Check cache
                if query_hash in self._query_cache:
                    cache_entry = self._query_cache[query_hash]
                    if self._is_cache_valid(cache_entry):
                        logger.logjson("INFO", "Cache hit for query", {
                            "query": query,
                            "cache_time": cache_entry['timestamp']
                        })
                        return cache_entry['result']
                    else:
                        del self._query_cache[query_hash]
                
                # Optimize query
                optimized_query = self._optimize_query(query)
                
                # Execute the query
                result = self.connection.execute(optimized_query).fetchdf()
                
                # Convert result to dictionary
                result_dict = result.to_dict(orient='records')
                
                # Cache the result
                self._query_cache[query_hash] = {
                    'result': result_dict,
                    'timestamp': time.time()
                }
                
                # Log query execution
                logger.logjson("INFO", "Query executed successfully", {
                    "query": query,
                    "optimized_query": optimized_query,
                    "execution_time": time.time() - start_time,
                    "rows_returned": len(result_dict)
                })
                
                return result_dict
            except Exception as e:
                logger.logjson("ERROR", f"Error executing query: {str(e)}\n{traceback.format_exc()}")
                if "timeout" in str(e).lower():
                    raise ConnectionError("Database connection timeout")
                elif "syntax" in str(e).lower():
                    raise ValueError("Invalid SQL syntax")
                elif "permission" in str(e).lower():
                    raise PermissionError("Database permission denied")
                raise
        return _execute()

    def execute_query_stream(self, query: str, params: Optional[Dict[str, Any]] = None) -> Generator[Dict[str, Any], None, None]:
        """Execute a query and stream results"""
        @with_retry(max_attempts=3, base_delay=1.0, max_delay=10.0, circuit_breaker=self._circuit_breaker)
        @handle_database_error
        def _execute():
            try:
                start_time = time.time()
                optimized_query = self._optimize_query(query)
                
                # Execute query and get result iterator
                result_iterator = self.connection.execute(optimized_query).fetchdf().iterrows()
                
                for _, row in result_iterator:
                    yield dict(row)  # Convert pandas Series to dict
                
                logger.logjson("INFO", "Query stream completed", {
                    "query": query,
                    "execution_time": time.time() - start_time
                })
            except Exception as e:
                logger.logjson("ERROR", f"Error streaming query results: {str(e)}\n{traceback.format_exc()}")
                raise
        yield from _execute()

    def query_table(self, table_name: str, columns: Optional[List[str]] = None, 
                   filters: Optional[Dict[str, Any]] = None, 
                   limit: Optional[int] = None,
                   order_by: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Query a table with optional column selection, filters, limit, and ordering
        """
        @with_retry(max_attempts=3, base_delay=1.0, max_delay=10.0, circuit_breaker=self._circuit_breaker)
        @handle_database_error
        def _execute():
            try:
                query = build_select_query(
                    table_name=table_name,
                    columns=columns,
                    filters=filters,
                    limit=limit,
                    order_by=order_by
                )
                return self.execute_query(query)
            except Exception as e:
                logger.logjson("ERROR", f"Error querying table {table_name}: {str(e)}")
                return None
        return _execute()

    def get_table_stats(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get statistics about a table including row count, column stats, etc.
        """
        @with_retry(max_attempts=3, base_delay=1.0, max_delay=10.0, circuit_breaker=self._circuit_breaker)
        @handle_database_error
        def _execute():
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
                logger.logjson("ERROR", f"Error getting table stats for {table_name}: {str(e)}")
                return None
        return _execute()

    def get_table_sample(self, table_name: str, sample_size: int = 5) -> Optional[Dict[str, Any]]:
        """
        Get a random sample of rows from a table
        """
        @with_retry(max_attempts=3, base_delay=1.0, max_delay=10.0, circuit_breaker=self._circuit_breaker)
        @handle_database_error
        def _execute():
            try:
                query = f"""
                    SELECT *
                    FROM s3tables.{table_name}
                    ORDER BY random()
                    LIMIT {sample_size};
                """
                return self.execute_query(query)
                
            except Exception as e:
                logger.logjson("ERROR", f"Error getting table sample for {table_name}: {str(e)}")
                return None
        return _execute()

    def get_column_distinct_values(self, table_name: str, column_name: str, 
                                 limit: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Get distinct values in a column with their counts
        """
        @with_retry(max_attempts=3, base_delay=1.0, max_delay=10.0, circuit_breaker=self._circuit_breaker)
        @handle_database_error
        def _execute():
            try:
                query = build_distinct_values_query(table_name, column_name, limit)
                return self.execute_query(query)
                
            except Exception as e:
                logger.logjson("ERROR", f"Error getting distinct values for {column_name}: {str(e)}")
                return None
        return _execute()

    @handle_database_error
    def close(self):
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                logger.logjson("ERROR", f"Error closing connection: {str(e)}")

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

    def clear_table_cache(self, table_name: Optional[str] = None):
        """Clear the table cache, optionally for a specific table"""
        if table_name:
            # Remove specific table from cache
            keys_to_remove = [k for k in self._table_cache.keys() if table_name in k]
            for k in keys_to_remove:
                del self._table_cache[k]
                if k in self._table_cache_metadata:
                    del self._table_cache_metadata[k]
            logger.debug(f"Cleared cache for table {table_name}")
        else:
            # Clear entire cache
            self._table_cache.clear()
            self._table_cache_metadata.clear()
            logger.debug("Cleared entire table cache") 