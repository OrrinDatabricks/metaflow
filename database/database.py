"""
Database utilities for metadata enrichment tracking
"""
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Text, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from typing import List, Dict, Optional
import logging

from config import Config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

class MetadataItem(Base):
    """Database model for metadata items"""
    __tablename__ = 'metadata_items'
    __table_args__ = {'schema': Config.DB_SCHEMA}
    
    id = Column(Integer, primary_key=True)
    catalog_name = Column(String(255), nullable=False)  # Added catalog support
    schema_name = Column(String(255), nullable=False)   # Added schema support  
    table_name = Column(String(255), nullable=False)
    column_name = Column(String(255), nullable=True)  # NULL for table-level metadata
    metadata_type = Column(String(50), nullable=False)  # 'comment' or 'description'
    original_value = Column(Text)
    generated_value = Column(Text, nullable=False)
    current_value = Column(Text, nullable=False)
    status = Column(String(50), default='pending')  # pending, approved, rejected, edited
    generation_method = Column(String(100))  # 'openai_gpt4', 'claude', 'manual', etc.
    generation_prompt = Column(Text)  # Store the prompt used for generation
    generation_metadata = Column(Text)  # JSON metadata about generation (tokens, cost, etc.)
    data_sample = Column(Text)  # Sample data used for generation
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(255))
    reviewed_by = Column(String(255))
    reviewed_at = Column(DateTime)

class ApprovalHistory(Base):
    """Database model for approval history"""
    __tablename__ = 'approval_history'
    __table_args__ = {'schema': Config.DB_SCHEMA}
    
    id = Column(Integer, primary_key=True)
    metadata_item_id = Column(Integer, ForeignKey(f'{Config.DB_SCHEMA}.metadata_items.id'))
    action = Column(String(50), nullable=False)  # approved, rejected, edited
    old_value = Column(Text)
    new_value = Column(Text)
    comment = Column(Text)
    user_name = Column(String(255))
    timestamp = Column(DateTime, default=datetime.utcnow)

class DatabaseManager:
    """Database connection and operations manager"""
    
    def __init__(self):
        self.engine = None
        self.Session = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize database connection"""
        try:
            connection_url = Config.get_postgres_url()
            self.engine = create_engine(
                connection_url,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            self.Session = sessionmaker(bind=self.engine)
            logger.info("Database connection initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            st.error(f"Database connection failed: {e}")
    
    def create_tables(self):
        """Create database tables if they don't exist"""
        try:
            # Create schema if it doesn't exist
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {Config.DB_SCHEMA}"))
                conn.commit()
            
            # Create tables
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            st.error(f"Failed to create database tables: {e}")
            return False
    
    def get_metadata_items(self, status: Optional[str] = None, table_name: Optional[str] = None, 
                          catalog_name: Optional[str] = None, schema_name: Optional[str] = None) -> pd.DataFrame:
        """Get metadata items with optional filtering"""
        try:
            query = """
            SELECT 
                id,
                catalog_name,
                schema_name,
                table_name,
                column_name,
                metadata_type,
                original_value,
                generated_value,
                current_value,
                status,
                generation_method,
                generation_prompt,
                generation_metadata,
                data_sample,
                created_at,
                updated_at,
                created_by,
                reviewed_by,
                reviewed_at
            FROM {schema}.metadata_items
            WHERE 1=1
            """.format(schema=Config.DB_SCHEMA)
            
            params = {}
            if status:
                query += " AND status = :status"
                params['status'] = status
            if table_name:
                query += " AND table_name = :table_name"
                params['table_name'] = table_name
            if catalog_name:
                query += " AND catalog_name = :catalog_name"
                params['catalog_name'] = catalog_name
            if schema_name:
                query += " AND schema_name = :schema_name"
                params['schema_name'] = schema_name
            
            query += " ORDER BY created_at DESC"
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            
            return df
        except Exception as e:
            logger.error(f"Failed to get metadata items: {e}")
            st.error(f"Failed to retrieve metadata items: {e}")
            return pd.DataFrame()
    
    def insert_metadata_item(self, catalog_name: str, schema_name: str, table_name: str, 
                           column_name: Optional[str], metadata_type: str, 
                           original_value: Optional[str], generated_value: str, 
                           created_by: str, generation_method: Optional[str] = None,
                           generation_prompt: Optional[str] = None, 
                           generation_metadata: Optional[str] = None,
                           data_sample: Optional[str] = None) -> bool:
        """Insert a new metadata item"""
        try:
            session = self.Session()
            item = MetadataItem(
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                metadata_type=metadata_type,
                original_value=original_value,
                generated_value=generated_value,
                current_value=generated_value,
                created_by=created_by,
                generation_method=generation_method,
                generation_prompt=generation_prompt,
                generation_metadata=generation_metadata,
                data_sample=data_sample
            )
            session.add(item)
            session.commit()
            session.close()
            full_name = f"{catalog_name}.{schema_name}.{table_name}"
            if column_name:
                full_name += f".{column_name}"
            logger.info(f"Inserted metadata item for {full_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to insert metadata item: {e}")
            st.error(f"Failed to insert metadata item: {e}")
            return False
    
    def update_metadata_item(self, item_id: int, new_value: str, status: str, 
                           reviewed_by: str, comment: Optional[str] = None) -> bool:
        """Update a metadata item"""
        try:
            session = self.Session()
            item = session.query(MetadataItem).filter_by(id=item_id).first()
            if item:
                old_value = item.current_value
                item.current_value = new_value
                item.status = status
                item.reviewed_by = reviewed_by
                item.reviewed_at = datetime.utcnow()
                
                # Add to approval history
                history = ApprovalHistory(
                    metadata_item_id=item_id,
                    action=status,
                    old_value=old_value,
                    new_value=new_value,
                    comment=comment,
                    user_name=reviewed_by
                )
                session.add(history)
                session.commit()
                session.close()
                logger.info(f"Updated metadata item {item_id}")
                return True
            else:
                logger.error(f"Metadata item {item_id} not found")
                return False
        except Exception as e:
            logger.error(f"Failed to update metadata item: {e}")
            st.error(f"Failed to update metadata item: {e}")
            return False
    
    def get_approval_history(self, item_id: int) -> pd.DataFrame:
        """Get approval history for a metadata item"""
        try:
            query = """
            SELECT 
                action,
                old_value,
                new_value,
                comment,
                user_name,
                timestamp
            FROM {schema}.approval_history
            WHERE metadata_item_id = :item_id
            ORDER BY timestamp DESC
            """.format(schema=Config.DB_SCHEMA)
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={'item_id': item_id})
            
            return df
        except Exception as e:
            logger.error(f"Failed to get approval history: {e}")
            st.error(f"Failed to retrieve approval history: {e}")
            return pd.DataFrame()
    
    def get_summary_stats(self) -> Dict:
        """Get summary statistics"""
        try:
            query = """
            SELECT 
                status,
                COUNT(*) as count
            FROM {schema}.metadata_items
            GROUP BY status
            """.format(schema=Config.DB_SCHEMA)
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            stats = df.set_index('status')['count'].to_dict()
            stats['total'] = sum(stats.values())
            return stats
        except Exception as e:
            logger.error(f"Failed to get summary stats: {e}")
            return {'total': 0, 'pending': 0, 'approved': 0, 'rejected': 0, 'edited': 0}

# Global database manager instance
@st.cache_resource
def get_db_manager():
    """Get cached database manager instance"""
    return DatabaseManager()
