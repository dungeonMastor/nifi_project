#!/usr/bin/env python3
"""MCP Server for MongoDB and ClickHouse integration."""
import logging
import sys
from mcp.server.fastmcp import FastMCP

from .config import Config
from .db_clients import get_mongo_client, get_clickhouse_client, close_clients
from .mongo_tools import (
    mongo_find, mongo_aggregate, describe_mongo_collection,
    list_mongo_databases, list_mongo_collections,
    mongo_insert_one, mongo_insert_many,
    mongo_update_one, mongo_update_many,
    mongo_delete_one, mongo_delete_many,
    create_mongo_collection
)
from .clickhouse_tools import (
    clickhouse_query, clickhouse_schema, describe_clickhouse_table,
    list_clickhouse_databases, list_clickhouse_tables, clickhouse_execute
)
from .nifi_tools import list_nifi_processor_types, list_nifi_controller_services

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)

# Create FastMCP server instance
mcp = FastMCP("MongoDB-ClickHouse-Context")


@mcp.tool()
def ping() -> dict:
    """
    Health check tool to verify the MCP server is running and databases are accessible.
    
    Returns:
        Dictionary with status of MongoDB and ClickHouse connections
    """
    status = {
        "server": "running",
        "mongo": {"status": "unknown"},
        "clickhouse": {"status": "unknown"}
    }
    
    # Test MongoDB connection
    try:
        client = get_mongo_client()
        client.admin.command('ping')
        status["mongo"] = {"status": "connected"}
    except Exception as e:
        status["mongo"] = {"status": "error", "error": str(e)}
    
    # Test ClickHouse connection
    try:
        client = get_clickhouse_client()
        client.command("SELECT 1")
        status["clickhouse"] = {"status": "connected"}
    except Exception as e:
        status["clickhouse"] = {"status": "error", "error": str(e)}
    
    return status


@mcp.tool()
def inspect_database_server() -> dict:
    """
    Inspect the connected database servers to list available databases.
    
    Use this tool first to see what content is available in MongoDB and ClickHouse.
    
    Returns:
        Dictionary with lists of databases for both MongoDB and ClickHouse
    """
    result = {}
    
    # Mongo
    try:
        mongo_dbs = list_mongo_databases()
        result["mongo"] = mongo_dbs
    except Exception as e:
        result["mongo"] = {"error": str(e)}
        
    # ClickHouse
    try:
        ch_dbs = list_clickhouse_databases()
        result["clickhouse"] = ch_dbs
    except Exception as e:
        result["clickhouse"] = {"error": str(e)}
        
    return result


@mcp.tool()
def mongo_find_tool(
    collection: str,
    database: str = "",
    filter_query: str = "{}",
    projection: str = "",
    limit: int = 10,
    fields_of_interest: str = ""
) -> dict:
    """
    Find documents in a MongoDB collection.
    
    Use this tool whenever you need document details from MongoDB for a given ID, key, or filter.
    This is read-only and safe for context completion.
    
    Args:
        collection: Name of the collection to query
        database: Database name (optional, uses default from config if not provided)
        filter_query: JSON string representing the MongoDB filter query (e.g., '{"status": "active"}')
        projection: JSON string representing fields to include/exclude (e.g., '{"name": 1, "_id": 0}')
        limit: Maximum number of documents to return (max 100)
        fields_of_interest: Comma-separated list of fields to prioritize in response
    
    Returns:
        Dictionary with 'documents' list and 'count' of total matching documents
    """
    db = database if database else None
    proj = projection if projection else None
    fields = fields_of_interest if fields_of_interest else None
    
    logger.info(f"mongo_find called: collection={collection}, database={db}, limit={limit}")
    return mongo_find(
        collection=collection,
        database=db,
        filter_query=filter_query,
        projection=proj,
        limit=limit,
        fields_of_interest=fields
    )


@mcp.tool()
def mongo_aggregate_tool(
    collection: str,
    pipeline: str,
    database: str = "",
    limit: int = 50
) -> dict:
    """
    Execute an aggregation pipeline on a MongoDB collection.
    
    Use this for complex lookups, joins, grouping, or data transformations.
    This is read-only and safe for context completion.
    
    Args:
        collection: Name of the collection to query
        pipeline: JSON string representing the aggregation pipeline array
        database: Database name (optional, uses default from config if not provided)
        limit: Maximum number of result documents to return (max 100)
    
    Returns:
        Dictionary with 'results' list from the aggregation pipeline
    """
    db = database if database else None
    
    logger.info(f"mongo_aggregate called: collection={collection}, database={db}")
    return mongo_aggregate(
        collection=collection,
        pipeline=pipeline,
        database=db,
        limit=limit
    )


@mcp.tool()
def describe_mongo_collection_tool(
    collection: str,
    database: str = "",
    sample_size: int = 5
) -> dict:
    """
    Describe a MongoDB collection by examining its schema and sample documents.
    
    Use this tool to understand the structure of a collection before querying it.
    Helps the agent build correct queries by showing field names and types.
    
    Args:
        collection: Name of the collection to describe
        database: Database name (optional, uses default from config if not provided)
        sample_size: Number of sample documents to examine (max 10)
    
    Returns:
        Dictionary with schema information and sample documents
    """
    db = database if database else None
    
    logger.info(f"describe_mongo_collection called: collection={collection}, database={db}")
    return describe_mongo_collection(
        collection=collection,
        database=db,
        sample_size=sample_size
    )


@mcp.tool()
def list_mongo_collections_tool(database: str = "") -> dict:
    """
    List collections in a specific MongoDB database.
    
    Args:
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with list of collection names
    """
    db = database if database else None
    logger.info(f"list_mongo_collections called: database={db}")
    return list_mongo_collections(database=db)


@mcp.tool()
def create_mongo_collection_tool(collection: str, database: str = "") -> dict:
    """
    Create a new MongoDB collection explicitly.
    
    Args:
        collection: Name of the collection to create
        database: Database name (optional, uses default from config if not provided)
    """
    db = database if database else None
    logger.info(f"create_mongo_collection called: collection={collection}, database={db}")
    return create_mongo_collection(collection=collection, database=db)


@mcp.tool()
def mongo_insert_one_tool(collection: str, document: str, database: str = "") -> dict:
    """
    Insert a single document into a MongoDB collection.
    
    Args:
        collection: Name of the collection
        document: JSON string representing the document to insert
        database: Database name (optional, uses default)
    """
    db = database if database else None
    logger.info(f"mongo_insert_one called: collection={collection}")
    return mongo_insert_one(collection=collection, document=document, database=db)


@mcp.tool()
def mongo_insert_many_tool(collection: str, documents: str, database: str = "") -> dict:
    """
    Insert multiple documents into a MongoDB collection.
    
    Args:
        collection: Name of the collection
        documents: JSON string representing a list of documents
        database: Database name (optional, uses default)
    """
    db = database if database else None
    logger.info(f"mongo_insert_many called: collection={collection}")
    return mongo_insert_many(collection=collection, documents=documents, database=db)


@mcp.tool()
def mongo_update_one_tool(collection: str, filter_query: str, update: str, database: str = "") -> dict:
    """
    Update a single document in a MongoDB collection.
    
    Args:
        collection: Name of the collection
        filter_query: JSON string representing the filter
        update: JSON string representing the update operations
        database: Database name (optional, uses default)
    """
    db = database if database else None
    logger.info(f"mongo_update_one called: collection={collection}")
    return mongo_update_one(collection=collection, filter_query=filter_query, update=update, database=db)


@mcp.tool()
def mongo_update_many_tool(collection: str, filter_query: str, update: str, database: str = "") -> dict:
    """
    Update multiple documents in a MongoDB collection.
    
    Args:
        collection: Name of the collection
        filter_query: JSON string representing the filter
        update: JSON string representing the update operations
        database: Database name (optional, uses default)
    """
    db = database if database else None
    logger.info(f"mongo_update_many called: collection={collection}")
    return mongo_update_many(collection=collection, filter_query=filter_query, update=update, database=db)


@mcp.tool()
def mongo_delete_one_tool(collection: str, filter_query: str, database: str = "") -> dict:
    """
    Delete a single document from a MongoDB collection.
    
    Args:
        collection: Name of the collection
        filter_query: JSON string representing the filter
        database: Database name (optional, uses default)
    """
    db = database if database else None
    logger.info(f"mongo_delete_one called: collection={collection}")
    return mongo_delete_one(collection=collection, filter_query=filter_query, database=db)


@mcp.tool()
def mongo_delete_many_tool(collection: str, filter_query: str, database: str = "") -> dict:
    """
    Delete multiple documents from a MongoDB collection.
    
    Args:
        collection: Name of the collection
        filter_query: JSON string representing the filter
        database: Database name (optional, uses default)
    """
    db = database if database else None
    logger.info(f"mongo_delete_many called: collection={collection}")
    return mongo_delete_many(collection=collection, filter_query=filter_query, database=db)


@mcp.tool()
def clickhouse_query_tool(
    query: str,
    params: str = "",
    row_limit: int = 100,
    columns_of_interest: str = ""
) -> dict:
    """
    Execute a SELECT query on ClickHouse.
    
    Use this for analytics, time-series data, aggregations, or any analytical queries.
    This is read-only and safe for context completion.
    
    Args:
        query: SQL SELECT query string
        params: Optional JSON string with parameters for parameterized queries
        row_limit: Maximum number of rows to return (max 1000)
        columns_of_interest: Comma-separated list of column names to prioritize
    
    Returns:
        Dictionary with 'rows' list and 'columns' metadata
    """
    p = params if params else None
    cols = columns_of_interest if columns_of_interest else None
    
    logger.info(f"clickhouse_query called: query_length={len(query)}, row_limit={row_limit}")
    return clickhouse_query(
        query=query,
        params=p,
        row_limit=row_limit,
        columns_of_interest=cols
    )


@mcp.tool()
def clickhouse_schema_tool(
    database: str = "",
    table: str = ""
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
    db = database if database else None
    tbl = table if table else None
    
    logger.info(f"clickhouse_schema called: database={db}, table={tbl}")
    return clickhouse_schema(
        database=db,
        table=tbl
    )


@mcp.tool()
def describe_clickhouse_table_tool(
    table: str,
    database: str = "",
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
    db = database if database else None
    
    logger.info(f"describe_clickhouse_table called: table={table}, database={db}")
    return describe_clickhouse_table(
        table=table,
        database=db,
        sample_rows=sample_rows
    )


@mcp.tool()
def list_clickhouse_tables_tool(database: str = "") -> dict:
    """
    List tables in a specific ClickHouse database.
    
    Args:
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with list of table names
    """
    db = database if database else None
    logger.info(f"list_clickhouse_tables called: database={db}")
    return list_clickhouse_tables(database=db)


@mcp.tool()
def clickhouse_execute_tool(query: str, params: str = "") -> dict:
    """
    Execute an arbitrary SQL query on ClickHouse (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, etc.).
    
    Args:
        query: SQL query string
        params: Optional JSON string with parameters for parameterized queries
    
    Returns:
        Dictionary with status and summary of operation
    """
    p = params if params else None
    logger.info(f"clickhouse_execute called: query_length={len(query)}")
    return clickhouse_execute(query=query, params=p)


@mcp.tool()
def nifi_list_processor_types_tool() -> dict:
    """
    List available processor types from the connected NiFi instance.

    Returns a list of processor types with their FQCN and bundle info.
    Requires NIFI_AUTH to be set in the environment.

    Returns:
        Dictionary with 'processor_types' list (each: {type, bundle})
    """
    auth = Config.NIFI_AUTH
    if not auth:
        return {"error": "NIFI_AUTH not configured", "processor_types": []}
    logger.info("nifi_list_processor_types called")
    try:
        types = list_nifi_processor_types(auth=auth)
        return {"processor_types": types, "count": len(types)}
    except Exception as e:
        return {"error": str(e), "processor_types": []}


@mcp.tool()
def nifi_list_controller_services_tool() -> dict:
    """
    List existing controller-service instances from the NiFi root process group.

    Returns running/configured CS instances with their id, type, name, and state.
    Requires NIFI_AUTH to be set in the environment.

    Returns:
        Dictionary with 'controller_services' list (each: {id, type, name, state})
    """
    auth = Config.NIFI_AUTH
    if not auth:
        return {"error": "NIFI_AUTH not configured", "controller_services": []}
    logger.info("nifi_list_controller_services called")
    try:
        services = list_nifi_controller_services(auth=auth)
        return {"controller_services": services, "count": len(services)}
    except Exception as e:
        return {"error": str(e), "controller_services": []}


def main():
    """Main entry point for the MCP server."""
    # Validate configuration
    is_valid, error_msg = Config.validate()
    if not is_valid:
        logger.error(f"Configuration error: {error_msg}")
        logger.error("Please set required environment variables:")
        logger.error("  - MONGO_URI (required)")
        logger.error("  - CLICKHOUSE_HOST (required)")
        logger.error("  - MONGO_DB (optional, default database)")
        logger.error("  - CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE (optional)")
        sys.exit(1)
    
    logger.info("Starting MongoDB-ClickHouse MCP Server...")
    logger.info(f"MongoDB URI: {Config.MONGO_URI[:20]}..." if len(Config.MONGO_URI) > 20 else f"MongoDB URI: {Config.MONGO_URI}")
    logger.info(f"ClickHouse Host: {Config.CLICKHOUSE_HOST}:{Config.CLICKHOUSE_PORT}")
    
    # Run the server (default transport is stdio for Cursor/IDE clients)
    try:
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.exception("MCP server exited with error: %s", e)
        sys.exit(1)
    finally:
        close_clients()


if __name__ == "__main__":
    main()

