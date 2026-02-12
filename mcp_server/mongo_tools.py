"""MongoDB MCP tools for querying and schema discovery."""
import json
import logging
from typing import Optional
from pymongo.errors import OperationFailure, PyMongoError

from .db_clients import get_mongo_client
from .config import Config

logger = logging.getLogger(__name__)


def mongo_find(
    collection: str,
    database: Optional[str] = None,
    filter_query: Optional[str] = None,
    projection: Optional[str] = None,
    limit: int = 10,
    fields_of_interest: Optional[str] = None
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
        limit: Maximum number of documents to return (no additional server-side cap)
        fields_of_interest: Comma-separated list of fields to prioritize in response
    
    Returns:
        Dictionary with 'documents' list and 'count' of total matching documents
    """
    try:
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        coll = db[collection]
        
        # Parse filter query
        filter_dict = {}
        if filter_query:
            try:
                filter_dict = json.loads(filter_query)
            except json.JSONDecodeError as e:
                return {"error": f"Invalid JSON in filter_query: {e}"}
        
        # Parse projection
        projection_dict = None
        if projection:
            try:
                projection_dict = json.loads(projection)
            except json.JSONDecodeError as e:
                return {"error": f"Invalid JSON in projection: {e}"}
        
        # Execute query
        cursor = coll.find(filter_dict, projection_dict).limit(limit)
        documents = list(cursor)
        
        # Get total count (no additional artificial limit; relies on DB permissions)
        total_count = coll.count_documents(filter_dict)
        
        # Filter fields of interest if specified
        if fields_of_interest and documents:
            fields = [f.strip() for f in fields_of_interest.split(",")]
            filtered_docs = []
            for doc in documents:
                filtered_doc = {k: v for k, v in doc.items() if k in fields or k == "_id"}
                if filtered_doc:
                    filtered_docs.append(filtered_doc)
            documents = filtered_docs if filtered_docs else documents
        
        # Convert ObjectId to string for JSON serialization
        for doc in documents:
            if "_id" in doc and hasattr(doc["_id"], "__str__"):
                doc["_id"] = str(doc["_id"])
        
        return {
            "documents": documents,
            "count": len(documents),
            "total_matching": total_count,
            "collection": collection,
            "database": db_name
        }
    
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        return {"error": f"MongoDB operation failed: {str(e)}"}
    except PyMongoError as e:
        logger.error(f"MongoDB error: {e}")
        return {"error": f"MongoDB error: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected error in mongo_find: {e}")
        return {"error": f"Unexpected error: {str(e)}"}


def mongo_aggregate(
    collection: str,
    pipeline: str,
    database: Optional[str] = None,
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
        limit: Optional $limit to apply at the end of the pipeline
    
    Returns:
        Dictionary with 'results' list from the aggregation pipeline
    """
    try:
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        coll = db[collection]
        
        # Parse pipeline
        try:
            pipeline_list = json.loads(pipeline)
            if not isinstance(pipeline_list, list):
                return {"error": "Pipeline must be a JSON array"}
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON in pipeline: {e}"}
        
        # Add $limit stage if not present and limit is specified
        has_limit = any(stage.get("$limit") for stage in pipeline_list if isinstance(stage, dict))
        if not has_limit and limit > 0:
            pipeline_list.append({"$limit": limit})
        
        # Execute aggregation
        results = list(coll.aggregate(pipeline_list))
        
        # Convert ObjectId to string
        for doc in results:
            if "_id" in doc and hasattr(doc["_id"], "__str__"):
                doc["_id"] = str(doc["_id"])
        
        return {
            "results": results,
            "count": len(results),
            "collection": collection,
            "database": db_name
        }
    
    except OperationFailure as e:
        logger.error(f"MongoDB aggregation failed: {e}")
        return {"error": f"MongoDB aggregation failed: {str(e)}"}
    except PyMongoError as e:
        logger.error(f"MongoDB error: {e}")
        return {"error": f"MongoDB error: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected error in mongo_aggregate: {e}")
        return {"error": f"Unexpected error: {str(e)}"}


def describe_mongo_collection(
    collection: str,
    database: Optional[str] = None,
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
    try:
        sample_size = min(sample_size, 10)
        
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        coll = db[collection]
        
        # Get collection stats
        stats = db.command("collStats", collection)
        count = coll.count_documents({})
        
        # Get sample documents
        samples = list(coll.find().limit(sample_size))
        
        # Analyze schema from samples
        field_types = {}
        for doc in samples:
            for key, value in doc.items():
                if key not in field_types:
                    field_types[key] = set()
                field_types[key].add(type(value).__name__)
        
        schema = {
            key: list(types) for key, types in field_types.items()
        }
        
        # Convert ObjectId to string in samples
        for doc in samples:
            if "_id" in doc and hasattr(doc["_id"], "__str__"):
                doc["_id"] = str(doc["_id"])
        
        return {
            "collection": collection,
            "database": db_name,
            "document_count": count,
            "size_bytes": stats.get("size", 0),
            "schema": schema,
            "sample_documents": samples,
            "sample_count": len(samples)
        }
    
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        return {"error": f"MongoDB operation failed: {str(e)}"}
    except PyMongoError as e:
        logger.error(f"MongoDB error: {e}")
        return {"error": f"MongoDB error: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected error in describe_mongo_collection: {e}")
        return {"error": f"Unexpected error: {str(e)}"}


def list_mongo_databases() -> dict:
    """
    List all databases in the MongoDB instance.
    
    Returns:
        Dictionary with list of database names
    """
    try:
        client = get_mongo_client()
        dbs = client.list_database_names()
        return {"databases": dbs, "count": len(dbs)}
    except Exception as e:
        logger.error(f"Error listing databases: {e}")
        return {"error": str(e)}


def list_mongo_collections(database: str = "") -> dict:
    """
    List collections in a specific MongoDB database.
    
    Args:
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with list of collection names
    """
    try:
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        collections = db.list_collection_names()
        return {"collections": collections, "count": len(collections), "database": db_name}
    except Exception as e:
        logger.error(f"Error listing collections: {e}")
        return {"error": str(e)}


def create_mongo_collection(collection: str, database: str = "") -> dict:
    """
    Create a new MongoDB collection explicitly.
    
    Args:
        collection: Name of the collection to create
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with status of the operation
    """
    try:
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        db.create_collection(collection)
        return {"status": "success", "collection": collection, "database": db_name}
    except Exception as e:
        logger.error(f"Error creating collection: {e}")
        return {"error": str(e)}


def mongo_insert_one(collection: str, document: str, database: str = "") -> dict:
    """
    Insert a single document into a MongoDB collection.
    
    Args:
        collection: Name of the collection
        document: JSON string representing the document to insert
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with inserted_id and status
    """
    try:
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        coll = db[collection]
        
        try:
            doc = json.loads(document)
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON in document: {e}"}
        
        result = coll.insert_one(doc)
        return {
            "status": "success",
            "inserted_id": str(result.inserted_id),
            "collection": collection,
            "database": db_name
        }
    except Exception as e:
        logger.error(f"Error inserting document: {e}")
        return {"error": str(e)}


def mongo_insert_many(collection: str, documents: str, database: str = "") -> dict:
    """
    Insert multiple documents into a MongoDB collection.
    
    Args:
        collection: Name of the collection
        documents: JSON string representing a list of documents to insert
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with inserted_ids and status
    """
    try:
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        coll = db[collection]
        
        try:
            docs = json.loads(documents)
            if not isinstance(docs, list):
                return {"error": "Documents must be a JSON list"}
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON in documents: {e}"}
        
        result = coll.insert_many(docs)
        return {
            "status": "success",
            "inserted_ids": [str(id) for id in result.inserted_ids],
            "count": len(result.inserted_ids),
            "collection": collection,
            "database": db_name
        }
    except Exception as e:
        logger.error(f"Error inserting documents: {e}")
        return {"error": str(e)}


def mongo_update_one(collection: str, filter_query: str, update: str, database: str = "") -> dict:
    """
    Update a single document in a MongoDB collection.
    
    Args:
        collection: Name of the collection
        filter_query: JSON string representing the filter
        update: JSON string representing the update operations
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with matched_count, modified_count, and status
    """
    try:
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        coll = db[collection]
        
        try:
            filt = json.loads(filter_query)
            upd = json.loads(update)
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON in filter or update: {e}"}
        
        result = coll.update_one(filt, upd)
        return {
            "status": "success",
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "collection": collection,
            "database": db_name
        }
    except Exception as e:
        logger.error(f"Error updating document: {e}")
        return {"error": str(e)}


def mongo_update_many(collection: str, filter_query: str, update: str, database: str = "") -> dict:
    """
    Update multiple documents in a MongoDB collection.
    
    Args:
        collection: Name of the collection
        filter_query: JSON string representing the filter
        update: JSON string representing the update operations
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with matched_count, modified_count, and status
    """
    try:
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        coll = db[collection]
        
        try:
            filt = json.loads(filter_query)
            upd = json.loads(update)
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON in filter or update: {e}"}
        
        result = coll.update_many(filt, upd)
        return {
            "status": "success",
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "collection": collection,
            "database": db_name
        }
    except Exception as e:
        logger.error(f"Error updating documents: {e}")
        return {"error": str(e)}


def mongo_delete_one(collection: str, filter_query: str, database: str = "") -> dict:
    """
    Delete a single document from a MongoDB collection.
    
    Args:
        collection: Name of the collection
        filter_query: JSON string representing the filter
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with deleted_count and status
    """
    try:
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        coll = db[collection]
        
        try:
            filt = json.loads(filter_query)
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON in filter: {e}"}
        
        result = coll.delete_one(filt)
        return {
            "status": "success",
            "deleted_count": result.deleted_count,
            "collection": collection,
            "database": db_name
        }
    except Exception as e:
        logger.error(f"Error deleting document: {e}")
        return {"error": str(e)}


def mongo_delete_many(collection: str, filter_query: str, database: str = "") -> dict:
    """
    Delete multiple documents from a MongoDB collection.
    
    Args:
        collection: Name of the collection
        filter_query: JSON string representing the filter
        database: Database name (optional, uses default from config if not provided)
    
    Returns:
        Dictionary with deleted_count and status
    """
    try:
        client = get_mongo_client()
        db_name = database or Config.get_mongo_db_name()
        if not db_name:
            return {"error": "Database name not specified and MONGO_DB not configured"}
        
        db = client[db_name]
        coll = db[collection]
        
        try:
            filt = json.loads(filter_query)
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON in filter: {e}"}
        
        result = coll.delete_many(filt)
        return {
            "status": "success",
            "deleted_count": result.deleted_count,
            "collection": collection,
            "database": db_name
        }
    except Exception as e:
        logger.error(f"Error deleting documents: {e}")
        return {"error": str(e)}
