"""ClickHouse MCP tools for querying and schema discovery."""
import json
import logging
from typing import Optional
from clickhouse_connect.driver.exceptions import DatabaseError, InterfaceError

from .db_clients import get_clickhouse_client
from .config import Config

logger = logging.getLogger(__name__)


def _validate_select_query(query: str) -> tuple[bool, Optional[str]]:
    if not query or not query.strip():
        return False, "Query must be a non-empty string"
    return True, None


def clickhouse_query(
    query: str,
    params: Optional[str] = None,
    row_limit: int = 100,
    columns_of_interest: Optional[str] = None
) -> dict:
    """
    Execute a SELECT query on ClickHouse.
    
    Use this for analytics, time-series data, aggregations, or any analytical queries.
    This is read-only and safe for context completion.
    
    Args:
        query: SQL query string (underlying DB user is read-only)
        params: Optional JSON string with parameters for parameterized queries
        row_limit: Maximum number of rows to return (no additional server-side cap)
        columns_of_interest: Comma-separated list of column names to prioritize
    
    Returns:
        Dictionary with 'rows' list and 'columns' metadata
    """
    try:
        # Minimal validation
        is_valid, error_msg = _validate_select_query(query)
        if not is_valid:
            return {"error": error_msg}
        
        # Add LIMIT if not present, using the provided row_limit only
        query_upper = query.strip().upper()
        if row_limit and "LIMIT" not in query_upper:
            query = f"{query.rstrip(';')} LIMIT {row_limit}"
        
        client = get_clickhouse_client()
        
        # Parse parameters if provided
        query_params = {}
        if params:
            try:
                query_params = json.loads(params)
            except json.JSONDecodeError as e:
                return {"error": f"Invalid JSON in params: {e}"}
        
        # Execute query
        result = client.query(query, parameters=query_params)
        
        # Get column names
        columns = result.column_names
        
        # Get rows as dictionaries
        rows = []
        for row in result.result_rows:
            row_dict = dict(zip(columns, row))
            
            # Filter columns of interest if specified
            if columns_of_interest:
                interest_cols = [c.strip() for c in columns_of_interest.split(",")]
                row_dict = {k: v for k, v in row_dict.items() if k in interest_cols}
            
            rows.append(row_dict)
        
        return {
            "rows": rows,
            "columns": columns,
            "row_count": len(rows),
            "query": query
        }
    
    except DatabaseError as e:
        logger.error(f"ClickHouse database error: {e}")
        return {"error": f"ClickHouse database error: {str(e)}"}
    except InterfaceError as e:
        logger.error(f"ClickHouse interface error: {e}")
        return {"error": f"ClickHouse interface error: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected error in clickhouse_query: {e}")
        return {"error": f"Unexpected error: {str(e)}"}


def clickhouse_schema(
    database: Optional[str] = None,
    table: Optional[str] = None
) -> dict:
    """
    Get schema information for ClickHouse databases and tables.
    
    Use this tool to discover available tables and their column structures.
    Helps the agent build correct queries by showing table and column names.
    
    Args:
        database: Database name (optional, uses default from config if not provided)
        table: Table name (optional, if not provided lists all tables in database)
    
    Returns:
        Dictionary with schema information
    """
    try:
        client = get_clickhouse_client()
        db_name = database or Config.CLICKHOUSE_DATABASE
        
        if table:
            # Get specific table schema
            query = f"""
                SELECT 
                    name as column_name,
                    type as column_type,
                    default_kind,
                    default_expression,
                    comment
                FROM system.columns
                WHERE database = '{db_name}' AND table = '{table}'
                ORDER BY position
            """
            
            result = client.query(query)
            columns = []
            for row in result.result_rows:
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "default_kind": row[2],
                    "default_expression": row[3],
                    "comment": row[4]
                })
            
            # Get table info
            info_query = f"""
                SELECT 
                    engine,
                    total_rows,
                    total_bytes,
                    metadata_modification_time
                FROM system.tables
                WHERE database = '{db_name}' AND name = '{table}'
            """
            info_result = client.query(info_query)
            table_info = {}
            if info_result.result_rows:
                row = info_result.result_rows[0]
                table_info = {
                    "engine": row[0],
                    "total_rows": row[1],
                    "total_bytes": row[2],
                    "metadata_modification_time": str(row[3]) if row[3] else None
                }
            
            return {
                "database": db_name,
                "table": table,
                "columns": columns,
                "table_info": table_info
            }
        else:
            # List all tables in database
            query = f"""
                SELECT 
                    name,
                    engine,
                    total_rows,
                    total_bytes
                FROM system.tables
                WHERE database = '{db_name}'
                ORDER BY name
            """
            
            result = client.query(query)
            tables = []
            for row in result.result_rows:
                tables.append({
                    "name": row[0],
                    "engine": row[1],
                    "total_rows": row[2],
                    "total_bytes": row[3]
                })
            
            return {
                "database": db_name,
                "tables": tables,
                "table_count": len(tables)
            }
    
    except DatabaseError as e:
        logger.error(f"ClickHouse database error: {e}")
        return {"error": f"ClickHouse database error: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected error in clickhouse_schema: {e}")
        return {"error": f"Unexpected error: {str(e)}"}


def describe_clickhouse_table(
    table: str,
    database: Optional[str] = None,
    sample_rows: int = 5
) -> dict:
    """
    Describe a ClickHouse table with schema and sample data.
    
    Use this tool to understand table structure and see example data.
    Helps the agent build correct queries by showing column names, types, and sample values.
    
    Args:
        table: Table name to describe
        database: Database name (optional, uses default from config if not provided)
        sample_rows: Number of sample rows to return (max 10)
    
    Returns:
        Dictionary with schema and sample data
    """
    try:
        sample_rows = min(sample_rows, 10)
        
        client = get_clickhouse_client()
        db_name = database or Config.CLICKHOUSE_DATABASE
        
        # Get schema
        schema_result = clickhouse_schema(database=db_name, table=table)
        if "error" in schema_result:
            return schema_result
        
        # Get sample data
        query = f"SELECT * FROM `{db_name}`.`{table}` LIMIT {sample_rows}"
        sample_result = clickhouse_query(query, row_limit=sample_rows)
        
        return {
            "database": db_name,
            "table": table,
            "schema": schema_result,
            "sample_data": sample_result.get("rows", []),
            "sample_count": len(sample_result.get("rows", []))
        }
    
    except Exception as e:
        logger.error(f"Unexpected error in describe_clickhouse_table: {e}")
        return {"error": f"Unexpected error: {str(e)}"}


def list_clickhouse_databases() -> dict:
    """
    List all databases in ClickHouse.
    
    Returns:
        Dictionary with list of database names
    """
    try:
        client = get_clickhouse_client()
        result = client.query("SHOW DATABASES")
        dbs = [row[0] for row in result.result_rows]
        return {"databases": dbs, "count": len(dbs)}
    except Exception as e:
        logger.error(f"Error listing databases: {e}")
        return {"error": str(e)}


def list_clickhouse_tables(database: str = "") -> dict:
    """
    List tables in a specific ClickHouse database.
    
    Args:
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with list of table names
    """
    try:
        client = get_clickhouse_client()
        db_name = database or Config.CLICKHOUSE_DATABASE
        
        result = client.query(f"SHOW TABLES FROM {db_name}")
        tables = [row[0] for row in result.result_rows]
        return {"tables": tables, "count": len(tables), "database": db_name}
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return {"error": str(e)}


def clickhouse_execute(query: str, params: str = "") -> dict:
    """
    Execute an arbitrary SQL query on ClickHouse (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, etc.).
    
    Args:
        query: SQL query string
        params: Optional JSON string with parameters for parameterized queries
    
    Returns:
        Dictionary with status and summary of operation
    """
    try:
        if not query or not query.strip():
            return {"error": "Query must be a non-empty string"}
        
        client = get_clickhouse_client()
        
        query_params = {}
        if params:
            try:
                query_params = json.loads(params)
            except json.JSONDecodeError as e:
                return {"error": f"Invalid JSON in params: {e}"}
        
        if query.strip().upper().startswith("SELECT"):
             return {"error": "Use clickhouse_query_tool for SELECT queries"}

        result = client.command(query, parameters=query_params)
        
        return {
            "status": "success",
            "summary": str(result) if result else "Operation completed successfully",
            "query_sample": query[:50] + "..." if len(query) > 50 else query
        }
    except DatabaseError as e:
        logger.error(f"ClickHouse database error: {e}")
        return {"error": f"ClickHouse database error: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected error in clickhouse_execute: {e}")
        return {"error": f"Unexpected error: {str(e)}"}
