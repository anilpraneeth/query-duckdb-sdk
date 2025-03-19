from typing import Dict, Any, Optional, List
import time
from datetime import datetime
from ..utils.logging_utils import get_logger
from ..config.config import DuckDBIcebergConfig
from ..services.database_service import DatabaseService
from ..services.s3tables_service import S3TablesService
from ..services.postgres_service import PostgresService
from ..services.metrics_service import MetricsService
import traceback

logger = get_logger(__name__)

class DuckDBIcebergApp:
    def __init__(self, config: DuckDBIcebergConfig):
        self.config = config
        self.db_service = DatabaseService(config)
        self.s3tables_service = S3TablesService(config)
        self.postgres_service = PostgresService(config) if config.postgres_enabled else None
        self.metrics_service = MetricsService(config)
        self._running = False

    def start(self):
        """Start the application"""
        try:
            self._running = True
            logger.logjson("INFO", "Starting DuckDB Iceberg Application")
            
            # Initialize services
            self.db_service.initialize()
            if self.postgres_service:
                logger.logjson("INFO", "PostgreSQL service initialized")
            
            # Start metrics collection
            self.metrics_service.start()
            
            # Main application loop
            while self._running:
                try:
                    # Process DuckDB tables
                    self._process_duckdb_tables()
                    
                    # Process PostgreSQL tables if enabled
                    if self.postgres_service:
                        self._process_postgres_tables()
                    
                    # Sleep for configured interval
                    time.sleep(self.config.query_interval)
                    
                except Exception as e:
                    logger.logjson("ERROR", f"Error in main loop: {str(e)}\n{traceback.format_exc()}")
                    time.sleep(self.config.error_retry_interval)
                    
        except Exception as e:
            logger.logjson("ERROR", f"Failed to start application: {str(e)}\n{traceback.format_exc()}")
            self.stop()

    def stop(self):
        """Stop the application"""
        try:
            self._running = False
            logger.logjson("INFO", "Stopping DuckDB Iceberg Application")
            
            # Stop metrics collection
            self.metrics_service.stop()
            
            # Close database connections
            self.db_service.close()
            if self.postgres_service:
                self.postgres_service.close()
                
            logger.logjson("INFO", "Application stopped successfully")
            
        except Exception as e:
            logger.logjson("ERROR", f"Error stopping application: {str(e)}\n{traceback.format_exc()}")

    def _process_duckdb_tables(self):
        """Process DuckDB tables"""
        try:
            # Get list of tables
            tables = self.db_service.get_tables()
            if not tables:
                logger.logjson("WARN", "No tables found in DuckDB")
                return
                
            for table in tables:
                try:
                    # Get table stats
                    stats = self.db_service.get_table_stats(table)
                    if stats:
                        self.metrics_service.record_table_stats("duckdb", table, stats)
                    
                    # Get sample data
                    sample = self.db_service.get_table_sample(table)
                    if sample:
                        self.metrics_service.record_table_sample("duckdb", table, sample)
                        
                except Exception as e:
                    logger.logjson("ERROR", f"Error processing DuckDB table {table}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.logjson("ERROR", f"Error processing DuckDB tables: {str(e)}")

    def _process_postgres_tables(self):
        """Process PostgreSQL tables"""
        try:
            # Get list of tables
            tables = self.postgres_service.execute_query("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE';
            """)
            
            if not tables:
                logger.logjson("WARN", "No tables found in PostgreSQL")
                return
                
            for table in tables:
                try:
                    table_name = table['table_name']
                    
                    # Get table stats
                    stats = self.postgres_service.get_table_stats(table_name)
                    if stats:
                        self.metrics_service.record_table_stats("postgres", table_name, stats)
                    
                    # Get sample data
                    sample = self.postgres_service.get_table_sample(table_name)
                    if sample:
                        self.metrics_service.record_table_sample("postgres", table_name, sample)
                        
                except Exception as e:
                    logger.logjson("ERROR", f"Error processing PostgreSQL table {table_name}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.logjson("ERROR", f"Error processing PostgreSQL tables: {str(e)}") 