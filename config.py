"""
Configuration settings for the Metadata Enrichment Tracking App
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Application configuration"""
    
    # Databricks settings
    DATABRICKS_SERVER_HOSTNAME = os.getenv('DATABRICKS_SERVER_HOSTNAME')
    DATABRICKS_HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH')
    DATABRICKS_ACCESS_TOKEN = os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    # Database settings
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'metadata_enrichment')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    DB_SCHEMA = os.getenv('DB_SCHEMA', 'metadata_enrichment')
    
    # Application settings
    APP_TITLE = "Metadata Enrichment Tracker"
    APP_ICON = "📊"
    
    @classmethod
    def get_postgres_url(cls):
        """Get PostgreSQL connection URL"""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
