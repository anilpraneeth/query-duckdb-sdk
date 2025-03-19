import logging
import json
from datetime import datetime
from typing import Any, Dict

class CustomLogger:
    """Custom logger that outputs JSON formatted logs"""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(name)
        
        # Set up console handler if not already configured
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            self.logger.addHandler(console_handler)
            self.logger.setLevel(logging.INFO)
    
    def logjson(self, level: str, message: str, data: Dict[str, Any] = None):
        """Log a message with JSON data"""
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "message": message
        }
        if data:
            log_data.update(data)
            
        self.logger.info(json.dumps(log_data))

    def log(self, level: int, message: str, exc_info=None):
        """Log a message with a specific level"""
        self.logger.log(level, message, exc_info=exc_info)

    def debug(self, message: str, exc_info=None):
        """Log a debug message"""
        self.logger.debug(message, exc_info=exc_info)

    def info(self, message: str, exc_info=None):
        """Log an info message"""
        self.logger.info(message, exc_info=exc_info)

    def warning(self, message: str, exc_info=None):
        """Log a warning message"""
        self.logger.warning(message, exc_info=exc_info)

    def error(self, message: str, exc_info=None):
        """Log an error message"""
        self.logger.error(message, exc_info=exc_info)

    def critical(self, message: str, exc_info=None):
        """Log a critical message"""
        self.logger.critical(message, exc_info=exc_info)

    def exception(self, message: str):
        """Log an exception with full traceback"""
        self.logger.exception(message)

def get_logger(name: str) -> CustomLogger:
    """Get a logger instance"""
    return CustomLogger(name)

def setup_logging():
    """Set up basic logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ) 