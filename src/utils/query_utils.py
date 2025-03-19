from typing import Dict, Any, Optional, List

def build_select_query(
    table_name: str,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    order_by: Optional[str] = None,
    schema: str = 's3tables'
) -> str:
    """
    Build a SELECT query with optional clauses
    
    Args:
        table_name: Name of the table to query
        columns: List of columns to select
        filters: Dictionary of column-value pairs for WHERE clause
        limit: Number of rows to return
        order_by: Column name to order by
        schema: Schema name (default: 's3tables')
        
    Returns:
        str: Constructed SQL query
    """
    # Build column selection
    cols = "*" if not columns else ", ".join(columns)
    
    # Build WHERE clause
    where_clause = ""
    if filters:
        conditions = []
        for col, val in filters.items():
            if isinstance(val, str):
                conditions.append(f"{col} = '{val}'")
            else:
                conditions.append(f"{col} = {val}")
        where_clause = "WHERE " + " AND ".join(conditions)
    
    # Build ORDER BY clause
    order_clause = f"ORDER BY {order_by}" if order_by else ""
    
    # Build LIMIT clause
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # Construct query
    query = f"""
        SELECT {cols}
        FROM {schema}.{table_name}
        {where_clause}
        {order_clause}
        {limit_clause}
    """.strip()
    
    return query + ";"

def build_stats_query(table_name: str, column_name: str, schema: str = 's3tables') -> str:
    """
    Build a query to get column statistics
    
    Args:
        table_name: Name of the table
        column_name: Name of the column to get stats for
        schema: Schema name (default: 's3tables')
        
    Returns:
        str: Constructed SQL query
    """
    query = f"""
        SELECT 
            MIN({column_name}) as min_value,
            MAX({column_name}) as max_value,
            AVG({column_name})::FLOAT as avg_value,
            COUNT(DISTINCT {column_name}) as unique_values
        FROM {schema}.{table_name}
        WHERE {column_name} IS NOT NULL
    """.strip()
    
    return query + ";"

def build_distinct_values_query(
    table_name: str,
    column_name: str,
    limit: Optional[int] = None,
    schema: str = 's3tables'
) -> str:
    """
    Build a query to get distinct values in a column
    
    Args:
        table_name: Name of the table
        column_name: Name of the column
        limit: Optional limit on number of results
        schema: Schema name (default: 's3tables')
        
    Returns:
        str: Constructed SQL query
    """
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
        SELECT 
            {column_name} as value,
            COUNT(*) as count
        FROM {schema}.{table_name}
        WHERE {column_name} IS NOT NULL
        GROUP BY {column_name}
        ORDER BY count DESC
        {limit_clause}
    """.strip()
    
    return query + ";" 