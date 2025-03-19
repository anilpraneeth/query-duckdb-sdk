import strawberry
from strawberry.fastapi import GraphQLRouter
from typing import List, Optional, Dict, Any
from src.utils.logging_utils import get_logger
from src.services.postgres_service import PostgresService
from src.services.iceberg_service import IcebergService

logger = get_logger(__name__)

@strawberry.type
class TableRow:
    timestamp: str
    metric_name: str
    metric_value: float
    category: str
    host: str

@strawberry.type
class QueryResult:
    data: List[TableRow]
    row_count: int
    execution_time: float

@strawberry.type
class S3Table:
    name: str
    location: str
    format: str

def create_query(postgres_service: PostgresService, iceberg_service: IcebergService):
    @strawberry.type
    class Query:
        @strawberry.field
        async def hello(self) -> str:
            return "Hello from Federated Query Layer!"

        @strawberry.field
        async def health(self) -> str:
            return "OK"
        
        @strawberry.field
        async def list_s3_tables(self) -> List[S3Table]:
            """List all S3 tables in the configured bucket"""
            try:
                tables = await iceberg_service.get_tables()
                return [
                    S3Table(
                        name=table["name"],
                        location=table["location"],
                        format=table["format"]
                    )
                    for table in tables
                ]
            except Exception as e:
                logger.logjson("ERROR", f"Error listing S3 tables: {str(e)}")
                raise

        @strawberry.field
        async def queryIcebergTable(self, tableName: str, limit: Optional[int] = 10) -> QueryResult:
            """Query an Iceberg table with optional limit"""
            try:
                import time
                start_time = time.time()
                
                # Build the query
                query = f"SELECT * FROM iceberg.{tableName} LIMIT {limit}"
                
                # Execute the query
                results = await iceberg_service.execute_query(query)
                
                # Convert results to TableRow objects
                table_rows = [
                    TableRow(
                        timestamp=str(row["timestamp"]),
                        metric_name=row["metric_name"],
                        metric_value=float(row["metric_value"]),
                        category=row["category"],
                        host=row["host"]
                    )
                    for row in results
                ]
                
                execution_time = time.time() - start_time
                
                return QueryResult(
                    data=table_rows,
                    row_count=len(table_rows),
                    execution_time=execution_time
                )
            except Exception as e:
                logger.logjson("ERROR", f"Error querying Iceberg table {tableName}: {str(e)}")
                raise

        @strawberry.field
        async def icebergQuery(self, query: str) -> QueryResult:
            """Execute a custom query against Iceberg tables"""
            try:
                import time
                start_time = time.time()
                
                # Execute the query
                results = await iceberg_service.execute_query(query)
                
                # Convert results to TableRow objects
                table_rows = [
                    TableRow(
                        timestamp=str(row["timestamp"]),
                        metric_name=row["metric_name"],
                        metric_value=float(row["metric_value"]),
                        category=row["category"],
                        host=row["host"]
                    )
                    for row in results
                ]
                
                execution_time = time.time() - start_time
                
                return QueryResult(
                    data=table_rows,
                    row_count=len(table_rows),
                    execution_time=execution_time
                )
            except Exception as e:
                logger.logjson("ERROR", f"Error executing Iceberg query: {str(e)}")
                raise

    return Query

def create_graphql_router(postgres_service: PostgresService, iceberg_service: IcebergService):
    Query = create_query(postgres_service, iceberg_service)
    schema = strawberry.Schema(query=Query)
    return GraphQLRouter(schema) 