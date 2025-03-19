from typing import Dict, Any, Optional, List
import time
from datetime import datetime
import json
import os
from ..utils.logging_utils import get_logger
import threading
import traceback

logger = get_logger(__name__)

class MetricsService:
    def __init__(self, config):
        self.config = config
        self.metrics: Dict[str, Any] = {
            'queries_executed': 0,
            'errors': 0,
            'tables_processed': 0,
            'maintenance_operations': 0,
            'active_queries': 0,
            'query_results': [],
            'table_stats': {},
            'table_samples': {},
            'last_update': None
        }
        self._running = False
        self._lock = threading.Lock()
        self._metrics_thread = None

    def start(self):
        """Start the metrics service"""
        try:
            self._running = True
            self._metrics_thread = threading.Thread(target=self._run_metrics_logging)
            self._metrics_thread.start()
            logger.logjson("INFO", "Metrics service started")
        except Exception as e:
            logger.logjson("ERROR", f"Failed to start metrics service: {str(e)}")
            self.stop()

    def stop(self):
        """Stop the metrics service"""
        try:
            self._running = False
            if self._metrics_thread:
                self._metrics_thread.join()
            logger.logjson("INFO", "Metrics service stopped")
        except Exception as e:
            logger.logjson("ERROR", f"Error stopping metrics service: {str(e)}")

    def _run_metrics_logging(self):
        """Run the metrics logging loop"""
        while self._running:
            try:
                with self._lock:
                    # Log current metrics
                    self._log_metrics()
                    
                    # Clean up old query results
                    self._cleanup_old_results()
                    
                    # Update last update timestamp
                    self.metrics['last_update'] = datetime.now().isoformat()
                
                # Sleep for configured interval
                time.sleep(self.config.metrics_interval)
                
            except Exception as e:
                logger.logjson("ERROR", f"Error in metrics logging loop: {str(e)}")
                time.sleep(self.config.error_retry_interval)

    def _log_metrics(self):
        """Log current metrics"""
        try:
            metrics_to_log = {
                'queries_executed': self.metrics['queries_executed'],
                'errors': self.metrics['errors'],
                'tables_processed': self.metrics['tables_processed'],
                'maintenance_operations': self.metrics['maintenance_operations'],
                'active_queries': self.metrics['active_queries'],
                'last_update': self.metrics['last_update']
            }
            
            logger.logjson("INFO", "Current metrics", json_obj=metrics_to_log)
            
        except Exception as e:
            logger.logjson("ERROR", f"Error logging metrics: {str(e)}")

    def _cleanup_old_results(self):
        """Clean up old query results"""
        try:
            current_time = datetime.now()
            self.metrics['query_results'] = [
                result for result in self.metrics['query_results']
                if (current_time - datetime.fromisoformat(result['timestamp'])).total_seconds() 
                <= self.config.metrics_retention_seconds
            ]
        except Exception as e:
            logger.logjson("ERROR", f"Error cleaning up old results: {str(e)}")

    def increment_metric(self, metric_name: str, value: int = 1):
        """Increment a metric counter"""
        with self._lock:
            if metric_name in self.metrics:
                self.metrics[metric_name] += value

    def record_table_stats(self, db_type: str, table_name: str, stats: Dict[str, Any]):
        """Record table statistics"""
        with self._lock:
            if db_type not in self.metrics['table_stats']:
                self.metrics['table_stats'][db_type] = {}
            self.metrics['table_stats'][db_type][table_name] = {
                'stats': stats,
                'timestamp': datetime.now().isoformat()
            }

    def record_table_sample(self, db_type: str, table_name: str, sample: List[Dict[str, Any]]):
        """Record table sample data"""
        with self._lock:
            if db_type not in self.metrics['table_samples']:
                self.metrics['table_samples'][db_type] = {}
            self.metrics['table_samples'][db_type][table_name] = {
                'sample': sample,
                'timestamp': datetime.now().isoformat()
            }

    def add_query_result(self, result: Dict[str, Any]):
        """Add a query result to the metrics"""
        with self._lock:
            self.metrics['query_results'].append(result)
            self.increment_metric('queries_executed')

    def get_metrics(self) -> Dict[str, Any]:
        """Get a copy of the current metrics"""
        with self._lock:
            return self.metrics.copy()

    def get_table_stats(self, db_type: str, table_name: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific table"""
        with self._lock:
            if (db_type in self.metrics['table_stats'] and 
                table_name in self.metrics['table_stats'][db_type]):
                return self.metrics['table_stats'][db_type][table_name]
            return None

    def get_table_sample(self, db_type: str, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """Get sample data for a specific table"""
        with self._lock:
            if (db_type in self.metrics['table_samples'] and 
                table_name in self.metrics['table_samples'][db_type]):
                return self.metrics['table_samples'][db_type][table_name]['sample']
            return None 