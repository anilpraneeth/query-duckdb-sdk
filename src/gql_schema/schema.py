import strawberry
from strawberry.fastapi import GraphQLRouter
from typing import List, Optional, Dict, Any
from src.utils.logging_utils import get_logger
from src.services.postgres_service import PostgresService
from src.services.iceberg_service import IcebergService
import json

logger = get_logger(__name__)

@strawberry.scalar(
    name="JSON",
    description="The `JSON` scalar type represents JSON values as specified by ECMA-404",
    specified_by_url="http://www.ecma-international.org/publications/files/ECMA-ST/ECMA-404.pdf",
)
class JSON:
    @staticmethod
    def serialize(value: Any) -> str:
        return json.dumps(value)

    @staticmethod
    def parse_value(value: str) -> Any:
        return json.loads(value)

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
class MaintenanceResult:
    success: bool
    message: str

@strawberry.type
class AnalyticsResult:
    success: bool
    message: str
    glue_catalog_id: Optional[str]
    sagemaker_enabled: bool

@strawberry.type
class RepartitionResult:
    success: bool
    message: str
    num_partitions: int
    partition_columns: List[str]
    row_count: int

@strawberry.type
class MaterializeResult:
    success: bool
    message: str
    row_count: int
    columns: List[str]

@strawberry.type
class S3Table:
    name: str
    location: str
    format: str

@strawberry.type
class TableSchema:
    name: str
    type: str

@strawberry.type
class PostgresTable:
    name: str
    row_count: int
    schema: List[TableSchema]

@strawberry.type
class PostgresQueryRow:
    @strawberry.field
    def values(self) -> Dict[str, str]:
        return self._values

    def __init__(self, values: Dict[str, Any]):
        self._values = {k: str(v) for k, v in values.items()}

@strawberry.type
class PostgresQueryResult:
    data: List[JSON]
    row_count: int
    execution_time: float

@strawberry.type
class ColumnStats:
    count: int
    distinct_count: int
    min_value: Optional[JSON]
    max_value: Optional[JSON]

@strawberry.type
class PostgresTableStats:
    total_rows: int
    column_stats: JSON
    schema: List[TableSchema]

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

        @strawberry.field
        async def configure_table_maintenance(self, table_name: str) -> MaintenanceResult:
            """Configure maintenance settings for an Iceberg table"""
            try:
                iceberg_service.configure_table_maintenance(table_name)
                return MaintenanceResult(
                    success=True,
                    message=f"Successfully configured maintenance for table {table_name}"
                )
            except Exception as e:
                logger.logjson("ERROR", f"Error configuring maintenance for table {table_name}: {str(e)}")
                return MaintenanceResult(
                    success=False,
                    message=f"Failed to configure maintenance: {str(e)}"
                )

        @strawberry.field
        async def setup_analytics_integration(self) -> AnalyticsResult:
            """Setup analytics service integration"""
            try:
                iceberg_service.setup_analytics_integration()
                return AnalyticsResult(
                    success=True,
                    message="Successfully configured analytics integration",
                    glue_catalog_id=iceberg_service.config.glue_catalog_id,
                    sagemaker_enabled=iceberg_service.config.sagemaker_integration
                )
            except Exception as e:
                logger.logjson("ERROR", f"Error setting up analytics integration: {str(e)}")
                return AnalyticsResult(
                    success=False,
                    message=f"Failed to setup analytics integration: {str(e)}",
                    glue_catalog_id=None,
                    sagemaker_enabled=False
                )

        @strawberry.field
        async def repartition_table(
            self,
            table_name: str,
            num_partitions: int,
            partition_by: Optional[List[str]] = None,
            namespace: Optional[str] = None
        ) -> RepartitionResult:
            """Repartition a table for better performance"""
            try:
                df = iceberg_service.repartition_table(
                    table_name=table_name,
                    num_partitions=num_partitions,
                    partition_by=partition_by,
                    namespace=namespace
                )
                
                return RepartitionResult(
                    success=True,
                    message=f"Successfully repartitioned table {table_name}",
                    num_partitions=num_partitions,
                    partition_columns=partition_by or [],
                    row_count=len(df)
                )
            except Exception as e:
                logger.logjson("ERROR", f"Error repartitioning table {table_name}: {str(e)}")
                return RepartitionResult(
                    success=False,
                    message=f"Failed to repartition table: {str(e)}",
                    num_partitions=0,
                    partition_columns=[],
                    row_count=0
                )

        @strawberry.field
        async def materialize_query(self, query: str) -> MaterializeResult:
            """Materialize a query result for better performance"""
            try:
                df = iceberg_service.materialize_query(query)
                
                return MaterializeResult(
                    success=True,
                    message="Successfully materialized query result",
                    row_count=len(df),
                    columns=df.columns.tolist()
                )
            except Exception as e:
                logger.logjson("ERROR", f"Error materializing query: {str(e)}")
                return MaterializeResult(
                    success=False,
                    message=f"Failed to materialize query: {str(e)}",
                    row_count=0,
                    columns=[]
                )

        @strawberry.field
        async def list_postgres_tables(self) -> List[PostgresTable]:
            """List all PostgreSQL tables with their schemas"""
            try:
                tables = await postgres_service.get_tables()
                result = []
                
                for table_name in tables:
                    stats = await postgres_service.get_table_stats(table_name)
                    schema_list = [
                        TableSchema(name=col_name, type=col_type)
                        for col_name, col_type in stats['schema'].items()
                    ]
                    result.append(PostgresTable(
                        name=table_name,
                        row_count=stats['total_rows'],
                        schema=schema_list
                    ))
                
                return result
            except Exception as e:
                logger.logjson("ERROR", f"Error listing PostgreSQL tables: {str(e)}")
                raise

        @strawberry.field
        async def postgresQuery(self, query: str) -> PostgresQueryResult:
            """Execute a custom query against PostgreSQL"""
            try:
                import time
                start_time = time.time()
                
                # Execute the query
                results = await postgres_service.execute_query(query)
                
                # Convert results to JSON-compatible format
                json_results = []
                for row in results:
                    json_row = {}
                    for key, value in dict(row).items():
                        if isinstance(value, (int, float, str, bool, type(None))):
                            json_row[key] = value
                        else:
                            json_row[key] = str(value)
                    json_results.append(json_row)
                
                execution_time = time.time() - start_time
                
                return PostgresQueryResult(
                    data=json_results,
                    row_count=len(results),
                    execution_time=execution_time
                )
            except Exception as e:
                logger.logjson("ERROR", f"Error executing PostgreSQL query: {str(e)}")
                raise

        @strawberry.field
        async def get_postgres_table_sample(self, table_name: str, limit: Optional[int] = 10) -> PostgresQueryResult:
            """Get a sample of rows from a PostgreSQL table"""
            try:
                import time
                start_time = time.time()
                
                # Get sample data
                results = await postgres_service.get_table_sample(table_name, limit)
                
                execution_time = time.time() - start_time
                
                return PostgresQueryResult(
                    data=results,
                    row_count=len(results),
                    execution_time=execution_time
                )
            except Exception as e:
                logger.logjson("ERROR", f"Error getting sample from table {table_name}: {str(e)}")
                raise

        @strawberry.field
        async def get_postgres_table_stats(self, table_name: str) -> PostgresTableStats:
            """Get statistics for a PostgreSQL table"""
            try:
                stats = await postgres_service.get_table_stats(table_name)
                schema_list = [
                    TableSchema(name=col_name, type=col_type)
                    for col_name, col_type in stats['schema'].items()
                ]
                
                # Convert column stats to JSON-compatible format
                json_column_stats = {}
                for col_name, col_stats in stats['column_stats'].items():
                    json_stats = {}
                    for key, value in col_stats.items():
                        if isinstance(value, (int, float, str, bool, type(None))):
                            json_stats[key] = value
                        else:
                            json_stats[key] = str(value)
                    json_column_stats[col_name] = json_stats
                
                return PostgresTableStats(
                    total_rows=stats['total_rows'],
                    column_stats=json_column_stats,
                    schema=schema_list
                )
            except Exception as e:
                logger.logjson("ERROR", f"Error getting stats for table {table_name}: {str(e)}")
                raise

    return Query

def create_graphql_router(postgres_service: PostgresService, iceberg_service: IcebergService):
    Query = create_query(postgres_service, iceberg_service)
    schema = strawberry.Schema(query=Query)
    return GraphQLRouter(schema) 