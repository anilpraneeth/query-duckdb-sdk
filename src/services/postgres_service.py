from typing import List, Dict, Any, Optional
import asyncpg
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

class PostgresService:
    """Service for interacting with PostgreSQL database"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str,
                 ssl_mode: str = 'prefer', pool_size: int = 5):
        """Initialize the PostgreSQL service"""
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.ssl_mode = ssl_mode
        self.pool_size = pool_size
        self.pool = None
        self.logger = get_logger(__name__)
        
    async def connect(self):
        """Create a connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                ssl=self.ssl_mode,
                min_size=1,
                max_size=self.pool_size
            )
            self.logger.logjson("INFO", "Successfully connected to PostgreSQL")
        except Exception as e:
            self.logger.logjson("ERROR", f"Failed to connect to PostgreSQL: {str(e)}")
            raise
            
    async def close(self):
        """Close the connection pool"""
        if self.pool:
            await self.pool.close()
            self.logger.logjson("INFO", "Closed PostgreSQL connection pool")
            
    async def execute_query(self, query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query against PostgreSQL"""
        try:
            if not self.pool:
                await self.connect()
                
            async with self.pool.acquire() as conn:
                if params:
                    result = await conn.fetch(query, *params)
                else:
                    result = await conn.fetch(query)
                    
                return [dict(row) for row in result]
        except Exception as e:
            self.logger.logjson("ERROR", f"Error executing PostgreSQL query: {str(e)}")
            raise
            
    async def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for a PostgreSQL table"""
        try:
            if not self.pool:
                await self.connect()
                
            async with self.pool.acquire() as conn:
                # Get total rows
                total_rows = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                
                # Get column statistics
                column_stats = {}
                columns = await conn.fetch("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = $1
                """, table_name)
                
                for col in columns:
                    stats = await conn.fetchrow(f"""
                        SELECT 
                            COUNT(*) as count,
                            COUNT(DISTINCT {col['column_name']}) as distinct_count,
                            MIN({col['column_name']}) as min_value,
                            MAX({col['column_name']}) as max_value
                        FROM {table_name}
                    """)
                    column_stats[col['column_name']] = dict(stats)
                    
                return {
                    'total_rows': total_rows,
                    'column_stats': column_stats,
                    'schema': {col['column_name']: col['data_type'] for col in columns}
                }
        except Exception as e:
            self.logger.logjson("ERROR", f"Error getting PostgreSQL table stats: {str(e)}")
            raise
            
    async def get_tables(self) -> List[str]:
        """List all available PostgreSQL tables"""
        try:
            if not self.pool:
                await self.connect()
                
            async with self.pool.acquire() as conn:
                tables = await conn.fetch("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                return [table['table_name'] for table in tables]
        except Exception as e:
            self.logger.logjson("ERROR", f"Error listing PostgreSQL tables: {str(e)}")
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
            if not self.pool:
                await self.connect()
                
            async with self.pool.acquire() as conn:
                # Get sample data
                result = await conn.fetch(f"""
                    SELECT * FROM {table_name}
                    ORDER BY RANDOM()
                    LIMIT {limit}
                """)
                
                return [dict(row) for row in result]
                
        except Exception as e:
            self.logger.logjson("ERROR", f"Error getting sample from table {table_name}: {str(e)}")
            raise 