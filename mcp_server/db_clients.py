"""Database client initialization and management."""
import logging
from typing import Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError

try:
    from .config import Config
except ImportError:
    from config import Config

logger = logging.getLogger(__name__)

# Global clients (lazy initialization)
_mongo_client: Optional[MongoClient] = None
_clickhouse_client = None


def get_mongo_client() -> MongoClient:
    """Get or create MongoDB client (lazy initialization)."""
    global _mongo_client
    
    if _mongo_client is None:
        try:
            uri = Config.get_mongo_uri()
            if not uri:
                raise ValueError("MONGO_URI is not configured")
            
            _mongo_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            # Test connection
            _mongo_client.admin.command('ping')
            logger.info("MongoDB client connected successfully")
        except (ConnectionFailure, ConfigurationError, ValueError) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    return _mongo_client


def get_clickhouse_client():
    """Get or create ClickHouse client (lazy initialization)."""
    global _clickhouse_client
    
    if _clickhouse_client is None:
        try:
            config = Config.get_clickhouse_config()
            _clickhouse_client = clickhouse_connect.get_client(
                host=config["host"],
                port=config["port"],
                username=config["username"],
                password=config["password"],
                database=config["database"]
            )
            # Test connection
            _clickhouse_client.command("SELECT 1")
            logger.info("ClickHouse client connected successfully")
        except (DatabaseError, Exception) as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    return _clickhouse_client


def close_clients():
    """Close all database connections."""
    global _mongo_client, _clickhouse_client
    
    if _mongo_client:
        _mongo_client.close()
        _mongo_client = None
        logger.info("MongoDB client closed")
    
    if _clickhouse_client:
        _clickhouse_client.close()
        _clickhouse_client = None
        logger.info("ClickHouse client closed")
