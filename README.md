# Federated Query Layer

A GraphQL API service that provides a unified query interface for both PostgreSQL and Iceberg tables, designed to run on AWS ECS Fargate.

## Overview

This application serves as a federated query layer that allows you to:
- Query PostgreSQL databases (hot data, last 30 days) through GraphQL
- Query Iceberg tables in S3 (cold data, up to 2 years) through GraphQL
- Execute queries across both data sources in a single request
- Monitor query performance and health
- Automatically determine the appropriate data source based on query date

## Features

- GraphQL API for unified data access
- PostgreSQL database support (30-day retention)
- Iceberg tables support (2-year retention)
- Query performance monitoring
- JSON-formatted logging
- Containerized for AWS ECS Fargate
- Data source recommendation based on query date

## Prerequisites

- Python 3.8 or higher
- AWS credentials with appropriate permissions
- PostgreSQL database
- S3 bucket with Iceberg tables

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd federated-query-layer
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Docker and ECS Deployment

The application is containerized and configured for deployment on AWS ECS Fargate. The deployment configuration includes:

### Docker Configuration
- `Dockerfile`: Defines the container image build process
- `docker-compose.yml`: Local development and testing configuration
- `.dockerignore`: Specifies files to exclude from the Docker build

### ECS Configuration
The `ecs/` directory contains the necessary configuration files for AWS ECS deployment:
- `task-definition.json`: Defines the container and task specifications
- `service-definition.json`: Configures the ECS service settings

### Building and Deploying

1. Build the Docker image:
```bash
docker build -t federated-query-layer .
```

2. Test locally using docker-compose:
```bash
docker-compose up
```

3. Deploy to ECS:
```bash
# Register the task definition
aws ecs register-task-definition --cli-input-json file://ecs/task-definition.json

# Create/update the service
aws ecs create-service --cli-input-json file://ecs/service-definition.json
```

### ECS Task Definition
The task definition includes:
- Container image configuration
- CPU and memory allocation
- Environment variables
- Logging configuration
- Health check settings

### ECS Service Configuration
The service definition includes:
- Desired task count
- Load balancer configuration
- Auto-scaling settings
- Network configuration

## Configuration

Create a `.env` file in the project root with the following variables:

```env
# AWS Configuration
AWS_REGION=us-west-2
AWS_SECRET_NAME=your-secret-name
S3_TABLE_BUCKET=your-bucket-name

# Application Settings
QUERY_TIMEOUT=300
MAX_RETRIES=3
RETRY_DELAY=5
METRICS_INTERVAL=60
METRICS_RETENTION_SECONDS=3600

# PostgreSQL Configuration
POSTGRES_HOST=your-postgres-host
POSTGRES_PORT=5432
POSTGRES_DATABASE=your-database
POSTGRES_USER=your-username
POSTGRES_PASSWORD=your-password
POSTGRES_SSL_MODE=require
POSTGRES_POOL_SIZE=5

# Table Maintenance Configuration
MAINTENANCE_ENABLED=true
COMPACTION_ENABLED=true
SNAPSHOT_RETENTION_DAYS=30

# Analytics Integration Configuration
ANALYTICS_INTEGRATION_ENABLED=false
GLUE_CATALOG_ID=your-glue-catalog-id
SAGEMAKER_INTEGRATION=false

# Performance Optimization
DEFAULT_PARTITION_COUNT=10
MATERIALIZATION_ENABLED=true
MATERIALIZATION_TTL=3600

# API Settings
PORT=8000
HOST=0.0.0.0
```

## Usage

1. Start the API server:
```bash
python src/main.py
```

2. The GraphQL API will be available at `http://localhost:8000/graphql`

3. Example GraphQL queries:

```graphql
# Query PostgreSQL data (last 30 days)
query {
  postgresQuery(query: "SELECT * FROM users LIMIT 5") {
    data
    execution_time
    row_count
    data_source
    data_retention {
      postgres_retention_days
      iceberg_retention_days
    }
  }
}

# Query Iceberg data (up to 2 years)
query {
  icebergQuery(query: "SELECT * FROM iceberg_db.sales LIMIT 5") {
    data
    execution_time
    row_count
    data_source
    data_retention {
      postgres_retention_days
      iceberg_retention_days
    }
  }
}

# Get table statistics
query {
  postgresTableStats(table_name: "users") {
    total_rows
    column_stats
    schema
    data_retention {
      postgres_retention_days
      iceberg_retention_days
    }
  }
  icebergTableStats(table_name: "sales") {
    total_rows
    column_stats
    schema
    data_retention {
      postgres_retention_days
      iceberg_retention_days
    }
  }
}

# List available tables
query {
  postgresTables
  icebergTables
}

# Get recommended data source for a specific date
query {
  getRecommendedDataSource(date: "2024-02-15T00:00:00")
}

# Configure table maintenance
mutation {
  configureTableMaintenance(tableName: "sales") {
    success
    message
  }
}

# Setup analytics integration
mutation {
  setupAnalyticsIntegration {
    success
    message
    glueCatalogId
    sagemakerEnabled
  }
}

# Repartition a table for better performance
mutation {
  repartitionTable(
    tableName: "sales"
    numPartitions: 10
    partitionBy: ["date", "region"]
    namespace: "default"
  ) {
    success
    message
    numPartitions
    partitionColumns
    rowCount
  }
}

# Materialize a query result
mutation {
  materializeQuery(query: "SELECT * FROM sales WHERE date >= '2024-01-01'") {
    success
    message
    rowCount
    columns
  }
}
```

## Project Structure

```
src/
├── api/
│   └── app.py             # FastAPI application
├── config/
│   └── config.py          # Configuration management
├── graphql/
│   └── schema.py          # GraphQL schema and resolvers
├── services/
│   ├── postgres_service.py    # PostgreSQL service
│   ├── iceberg_service.py     # Iceberg tables service
│   └── metrics_service.py     # Metrics service
├── utils/
│   ├── logging_utils.py   # Logging utilities
│   ├── aws_utils.py      # AWS utilities
│   └── query_utils.py    # Query building utilities
└── main.py               # Application entry point
```

## Logging

The application uses JSON logging for better parsing and analysis. Logs include:
- Query execution details
- Error messages
- Performance metrics
- API requests and responses

## Error Handling

- Automatic retries for failed operations
- Configurable retry intervals
- Detailed error logging
- Graceful shutdown on errors

## Data Retention

The application implements a tiered data retention strategy:
- PostgreSQL: Stores the most recent 30 days of data for fast access
- Iceberg: Stores historical data up to 2 years for long-term analysis

The API automatically indicates which data source is being used for each query and provides retention information in the response. Use the `getRecommendedDataSource` field to determine which data source to query based on your date requirements.

## Performance Optimization

The application includes several features for optimizing query performance:

### Table Repartitioning
- Automatically repartition tables based on primary keys or specified columns
- Configurable number of partitions
- Improves query performance for large datasets
- Supports custom partition columns

### Small Pond Repartitioning
- Optimizes table partitioning for frequently accessed data
- Automatically detects optimal partition columns based on primary keys or unique columns
- Supports custom partition columns for specific use cases
- Configurable number of partitions through environment variables
- Provides detailed feedback about repartitioning operations
- Example usage:
  ```graphql
  mutation {
    repartitionTable(
      tableName: "sales"
      numPartitions: 10
      partitionBy: ["date", "region"]
      namespace: "default"
    ) {
      success
      message
      numPartitions
      partitionColumns
      rowCount
    }
  }
  ```

### Query Materialization
- Cache complex query results for faster subsequent access
- Automatic materialization of JOIN and UNION operations
- Configurable cache TTL
- Reduces computation time for frequently accessed data

### Query Optimization
- Automatic LIMIT addition for non-aggregate queries
- Materialization hints for complex operations
- Intelligent caching of table data and query results
- Circuit breaker for error handling and retries

## Security Considerations

### Environment Variables
This project uses environment variables for sensitive configuration. Never commit the actual `.env` file to version control. Instead:

1. Copy `.env.sample` to `.env`:
   ```bash
   cp .env.sample .env
   ```

2. Update the `.env` file with your actual credentials

### AWS Credentials
- AWS credentials are required for accessing S3 and Iceberg tables
- Store credentials securely using environment variables or AWS credentials file
- Never commit AWS credentials to version control

### Database Credentials
- Database credentials are managed through environment variables
- The default PostgreSQL password in `docker-compose.yml` should be overridden using environment variables

### Logging
- Logs may contain sensitive information
- Configure appropriate log levels in production
- Review logging configuration in `src/configs/log.conf`

## License

This project is licensed under the MIT License - see the LICENSE file for details.