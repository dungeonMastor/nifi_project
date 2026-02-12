"""Configuration module for MongoDB and ClickHouse connections."""
import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class Config:
    """Configuration for database connections."""
    
    # MongoDB configuration
    MONGO_URI: str = os.getenv("MONGO_URI", "")
    MONGO_DB: str = os.getenv("MONGO_DB", "")
    
    # ClickHouse configuration
    CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST")
    CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT"))
    CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD")
    CLICKHOUSE_DATABASE: str = os.getenv("CLICKHOUSE_DATABASE")

    # NiFi configuration (optional — NiFi tools skip if not set)
    NIFI_BASE_URL: str = os.getenv("NIFI_BASE_URL", "https://localhost:8443")
    NIFI_AUTH: str = os.getenv("NIFI_AUTH", "")
    NIFI_VERIFY_SSL: bool = os.getenv("NIFI_VERIFY_SSL", "true").strip().lower() not in (
        "0", "false", "no", "off",
    )
    
    @classmethod
    def validate(cls) -> tuple[bool, Optional[str]]:
        """Validate that required configuration is present."""
        if not cls.MONGO_URI:
            return False, "MONGO_URI environment variable is not set"
        if not cls.CLICKHOUSE_HOST:
            return False, "CLICKHOUSE_HOST environment variable is not set"
        return True, None
    
    @classmethod
    def get_mongo_uri(cls) -> str:
        """Get MongoDB connection URI."""
        return cls.MONGO_URI
    
    @classmethod
    def get_mongo_db_name(cls) -> str:
        """Get default MongoDB database name."""
        return cls.MONGO_DB
    
    @classmethod
    def get_clickhouse_config(cls) -> dict:
        """Get ClickHouse connection configuration."""
        return {
            "host": cls.CLICKHOUSE_HOST,
            "port": cls.CLICKHOUSE_PORT,
            "username": cls.CLICKHOUSE_USER,
            "password": cls.CLICKHOUSE_PASSWORD,
            "database": cls.CLICKHOUSE_DATABASE,
        }
