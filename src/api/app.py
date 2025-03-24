from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter
from src.services.postgres_service import PostgresService
from src.services.iceberg_service import IcebergService
from src.gql_schema.schema import create_graphql_router
from src.utils.logging_utils import get_logger
import time

logger = get_logger(__name__)

def create_app(config):
    """Create and configure the FastAPI application"""
    app = FastAPI(
        title="Federated Query Layer",
        description="GraphQL API for querying PostgreSQL and Iceberg tables",
        version="1.0.0"
    )
    
    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # In production, replace with specific origins
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Initialize services
    postgres_service = PostgresService(
        host=config.postgres_host,
        port=config.postgres_port,
        database=config.postgres_database,
        user=config.postgres_user
    )
    
    iceberg_service = IcebergService(
        region=config.aws_region,
        config=config
    )
    
    # Add health check endpoint
    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        try:
            # Check database health
            db_health = await postgres_service.health_check()
            
            # Overall health status
            overall_status = "healthy" if db_health["status"] == "healthy" else "unhealthy"
            
            return {
                "status": overall_status,
                "database": db_health,
                "timestamp": time.time()
            }
        except Exception as e:
            logger.logjson("ERROR", f"Health check failed: {str(e)}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": time.time()
            }
    
    # Add metrics endpoint
    @app.get("/metrics")
    async def get_metrics():
        """Get application metrics"""
        try:
            return {
                "database": {
                    "pool_metrics": postgres_service.get_pool_metrics(),
                    "circuit_breaker": {
                        "status": "closed" if postgres_service.circuit_breaker.is_closed() else "open",
                        "failures": postgres_service.circuit_breaker.failures
                    },
                    "cache_metrics": postgres_service.get_cache_metrics()
                }
            }
        except Exception as e:
            logger.logjson("ERROR", f"Failed to get metrics: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # Create GraphQL router
    graphql_router = create_graphql_router(postgres_service, iceberg_service)
    
    # Include GraphQL router
    app.include_router(graphql_router, prefix="/graphql")
    
    @app.on_event("startup")
    async def startup_event():
        """Initialize services on startup"""
        try:
            # Test database connection
            await postgres_service.health_check()
            logger.logjson("INFO", "Application startup completed successfully")
        except Exception as e:
            logger.logjson("ERROR", f"Application startup failed: {str(e)}")
            raise
            
    @app.on_event("shutdown")
    async def shutdown_event():
        """Clean up resources on shutdown"""
        try:
            await postgres_service.close()
            logger.logjson("INFO", "Application shutdown completed successfully")
        except Exception as e:
            logger.logjson("ERROR", f"Application shutdown failed: {str(e)}")
            raise
            
    return app 