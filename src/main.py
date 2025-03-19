import os
import uvicorn
from src.config.config import FederatedQueryConfig
from src.api.app import create_app
from src.utils.logging_utils import setup_logging, get_logger

logger = get_logger(__name__)

def main():
    """Main entry point for the application"""
    try:
        # Load configuration
        config = FederatedQueryConfig()
        
        # Validate configuration
        if not config.validate():
            logger.logjson("ERROR", "Configuration validation failed")
            return
            
        # Create FastAPI application
        app = create_app(config)
        
        # Start the server
        uvicorn.run(
            app,
            host=config.host,
            port=config.port,
            log_level="info"
        )
        
    except Exception as e:
        logger.logjson("ERROR", f"Application failed to start: {str(e)}")
        raise

if __name__ == "__main__":
    main() 