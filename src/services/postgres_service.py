from typing import List, Dict, Any, Optional
import os
from sqlalchemy import text, event
from src.utils.logging_utils import get_logger
from src.utils.session_utils import SessionUtils, DatabaseSession
from src.utils.error_utils import CircuitBreaker
import time
import threading
import duckdb
import hashlib
import json

logger = get_logger(__name__)

class PostgresService:
    """Service for interacting with PostgreSQL database"""
    
    def __init__(self, host: str = None, port: int = None, database: str = None, user: str = None, 
                 password: str = None, ssl_mode: str = 'verify-full', pool_size: int = 50, async_conn: bool = False):
        """Initialize the PostgreSQL service"""
        # Set environment variables for database connection
        if host:
            os.environ['DB_HOSTNAME'] = str(host)
        if port:
            os.environ['DB_PORT'] = str(port)
        if database:
            os.environ['DB_NAME'] = str(database)
        if user:
            os.environ['USER'] = str(user)
        if password:
            os.environ['POSTGRES_PASSWORD'] = password
        if ssl_mode:
            os.environ['POSTGRES_SSL_MODE'] = ssl_mode
        if pool_size:
            os.environ['POSTGRES_POOL_SIZE'] = str(pool_size)
            
        self.session_utils = SessionUtils(async_conn=async_conn)
        
        # Initialize circuit breaker
        self.circuit_breaker = CircuitBreaker(
            threshold=5,  # Number of failures before opening
            reset_timeout=60.0  # Time in seconds before attempting to close
        )
        
        # Initialize DuckDB for caching
        self.cache_db = duckdb.connect(':memory:')
        self.cache_ttl = int(os.getenv('QUERY_CACHE_TTL', '300'))
        
        # Create cache table
        self.cache_db.execute("""
            CREATE TABLE IF NOT EXISTS query_cache (
                query_hash VARCHAR,
                result JSON,
                created_at TIMESTAMP,
                PRIMARY KEY (query_hash)
            )
        """)
        
        # Connection pool metrics
        self._pool_metrics = {
            'total_connections': 0,
            'checkedin_connections': 0,
            'checkedout_connections': 0,
            'overflow_connections': 0,
            'last_update': time.time()
        }
        self._metrics_lock = threading.Lock()
        
        # Cache metrics
        self._cache_metrics = {
            'hits': 0,
            'misses': 0,
            'last_update': time.time()
        }
        self._cache_lock = threading.Lock()
        
        # Set up pool event listeners
        if not async_conn:
            event.listen(self.session_utils.engine, 'checkout', self._handle_checkout)
            event.listen(self.session_utils.engine, 'checkin', self._handle_checkin)
            event.listen(self.session_utils.engine, 'connect', self._handle_connect)
            event.listen(self.session_utils.engine, 'close', self._handle_close)

    def _handle_checkout(self, dbapi_connection, connection_record, connection_proxy):
        """Handle connection checkout event"""
        with self._metrics_lock:
            self._pool_metrics['checkedout_connections'] += 1
            self._pool_metrics['last_update'] = time.time()

    def _handle_checkin(self, dbapi_connection, connection_record):
        """Handle connection checkin event"""
        with self._metrics_lock:
            self._pool_metrics['checkedin_connections'] += 1
            self._pool_metrics['last_update'] = time.time()

    def _handle_connect(self, dbapi_connection, connection_record, connection_proxy):
        """Handle new connection event"""
        with self._metrics_lock:
            self._pool_metrics['total_connections'] += 1
            self._pool_metrics['last_update'] = time.time()

    def _handle_close(self, dbapi_connection, connection_record, connection_proxy):
        """Handle connection close event"""
        with self._metrics_lock:
            self._pool_metrics['total_connections'] -= 1
            self._pool_metrics['last_update'] = time.time()

    def get_pool_metrics(self) -> Dict[str, Any]:
        """Get current connection pool metrics"""
        with self._metrics_lock:
            return self._pool_metrics.copy()

    def _get_query_hash(self, query: str, params: Optional[List[Any]] = None) -> str:
        """Generate a hash for the query and its parameters"""
        query_data = {
            'query': query,
            'params': params
        }
        return hashlib.sha256(json.dumps(query_data, sort_keys=True).encode()).hexdigest()

    def _get_cached_result(self, query_hash: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached query result if it exists and is not expired"""
        try:
            result = self.cache_db.execute("""
                SELECT result, created_at 
                FROM query_cache 
                WHERE query_hash = ?
            """, [query_hash]).fetchone()
            
            if result:
                created_at = result[1]
                if time.time() - created_at.timestamp() <= self.cache_ttl:
                    with self._cache_lock:
                        self._cache_metrics['hits'] += 1
                    return json.loads(result[0])
                else:
                    # Remove expired cache entry
                    self.cache_db.execute("""
                        DELETE FROM query_cache 
                        WHERE query_hash = ?
                    """, [query_hash])
            
            with self._cache_lock:
                self._cache_metrics['misses'] += 1
            return None
        except Exception as e:
            logger.logjson("ERROR", f"Error accessing cache: {str(e)}")
            return None

    def _cache_result(self, query_hash: str, result: List[Dict[str, Any]]):
        """Cache query result"""
        try:
            self.cache_db.execute("""
                INSERT OR REPLACE INTO query_cache (query_hash, result, created_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, [query_hash, json.dumps(result)])
        except Exception as e:
            logger.logjson("ERROR", f"Error caching result: {str(e)}")

    async def execute_query(self, query: str, params: Optional[List[Any]] = None, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Execute a query against PostgreSQL"""
        try:
            # Check circuit breaker status
            if not self.circuit_breaker.is_closed():
                raise ConnectionError("Circuit breaker is open - database operations are temporarily disabled")

            # Try to get from cache if enabled
            if use_cache:
                query_hash = self._get_query_hash(query, params)
                cached_result = self._get_cached_result(query_hash)
                if cached_result is not None:
                    return cached_result

            # Convert query string to SQLAlchemy text object
            sql = text(query)
            
            if self.session_utils.async_conn:
                async with DatabaseSession(self.session_utils) as session:
                    if params:
                        result = await session.execute(sql, params)
                    else:
                        result = await session.execute(sql)
            else:
                with DatabaseSession(self.session_utils) as session:
                    if params:
                        result = session.execute(sql, params)
                    else:
                        result = session.execute(sql)
                    
            # Convert SQLAlchemy result to list of dictionaries
            rows = []
            for row in result:
                # Handle both Row objects and KeyedTuples
                if hasattr(row, '_mapping'):
                    # SQLAlchemy Row object
                    rows.append(dict(row._mapping))
                elif hasattr(row, '_fields'):
                    # SQLAlchemy KeyedTuple
                    rows.append(dict(zip(row._fields, row)))
                else:
                    # Regular tuple, try to get column names from result
                    if hasattr(result, 'keys'):
                        keys = result.keys()
                        rows.append(dict(zip(keys, row)))
                    else:
                        # Last resort: use index as key
                        rows.append({str(i): v for i, v in enumerate(row)})
            
            # Cache the result if enabled
            if use_cache:
                self._cache_result(query_hash, rows)
            
            # Record success in circuit breaker
            self.circuit_breaker.record_success()
            return rows
        except Exception as e:
            # Record failure in circuit breaker
            self.circuit_breaker.record_failure()
            logger.logjson("ERROR", f"Error executing PostgreSQL query: {str(e)}")
            raise

    def get_cache_metrics(self) -> Dict[str, Any]:
        """Get cache performance metrics"""
        with self._cache_lock:
            total = self._cache_metrics['hits'] + self._cache_metrics['misses']
            hit_rate = (self._cache_metrics['hits'] / total * 100) if total > 0 else 0
            return {
                'hits': self._cache_metrics['hits'],
                'misses': self._cache_metrics['misses'],
                'hit_rate': hit_rate,
                'last_update': self._cache_metrics['last_update']
            }

    async def health_check(self) -> Dict[str, Any]:
        """Perform a health check of the database connection"""
        try:
            # Check circuit breaker status
            circuit_status = "closed" if self.circuit_breaker.is_closed() else "open"
            
            # Get pool metrics
            pool_metrics = self.get_pool_metrics()
            
            # Get cache metrics
            cache_metrics = self.get_cache_metrics()
            
            # Try a simple query
            await self.execute_query("SELECT 1")
            
            return {
                "status": "healthy",
                "circuit_breaker": circuit_status,
                "pool_metrics": pool_metrics,
                "cache_metrics": cache_metrics,
                "last_error": None
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "circuit_breaker": "open" if not self.circuit_breaker.is_closed() else "closed",
                "pool_metrics": self.get_pool_metrics(),
                "cache_metrics": self.get_cache_metrics(),
                "last_error": str(e)
            }

    async def close(self):
        """Close the database engine and cache"""
        if self.session_utils.engine:
            if self.session_utils.async_conn:
                await self.session_utils.engine.dispose()
            else:
                self.session_utils.engine.dispose()
            logger.logjson("INFO", "Closed database connection")
        
        if self.cache_db:
            self.cache_db.close()
            logger.logjson("INFO", "Closed cache connection")

    async def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for a PostgreSQL table"""
        try:
            stats = {}
            
            # Get total rows
            count_query = f"SELECT COUNT(*) as total FROM {table_name}"
            result = await self.execute_query(count_query)
            stats['total_rows'] = result[0]['total']
            
            # Get column information
            column_query = """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
            """
            columns = await self.execute_query(column_query, [table_name])
            
            # Get column statistics
            column_stats = {}
            for col in columns:
                col_name = col['column_name']
                stats_query = f"""
                    SELECT 
                        COUNT(*) as count,
                        COUNT(DISTINCT {col_name}) as distinct_count,
                        MIN({col_name}) as min_value,
                        MAX({col_name}) as max_value
                    FROM {table_name}
                """
                col_stats = await self.execute_query(stats_query)
                column_stats[col_name] = col_stats[0]
            
            return {
                'total_rows': stats['total_rows'],
                'column_stats': column_stats,
                'schema': {col['column_name']: col['data_type'] for col in columns}
            }
        except Exception as e:
            logger.logjson("ERROR", f"Error getting PostgreSQL table stats: {str(e)}")
            raise
            
    async def get_tables(self) -> List[str]:
        """List all available PostgreSQL tables"""
        try:
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """
            result = await self.execute_query(query)
            return [row['table_name'] for row in result]
        except Exception as e:
            logger.logjson("ERROR", f"Error listing PostgreSQL tables: {str(e)}")
            raise

    async def get_table_sample(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get a sample of rows from a PostgreSQL table
        
        Args:
            table_name: Name of the table to sample
            limit: Maximum number of rows to return
            
        Returns:
            List of dictionaries containing the sampled rows
        """
        try:
            query = f"""
                SELECT * FROM {table_name}
                ORDER BY RANDOM()
                LIMIT {limit}
            """
            return await self.execute_query(query)
        except Exception as e:
            logger.logjson("ERROR", f"Error getting sample from table {table_name}: {str(e)}")
            raise 