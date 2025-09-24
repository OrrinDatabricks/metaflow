"""
Utility functions for the Metadata Enrichment Tracker
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Optional
import os
import getpass

def format_timestamp(timestamp) -> str:
    """Format timestamp for display"""
    if pd.isna(timestamp):
        return "Never"
    
    if isinstance(timestamp, str):
        try:
            timestamp = pd.to_datetime(timestamp)
        except:
            return timestamp
    
    now = datetime.now()
    diff = now - timestamp.replace(tzinfo=None)
    
    if diff.days > 7:
        return timestamp.strftime("%Y-%m-%d %H:%M")
    elif diff.days > 0:
        return f"{diff.days} days ago"
    elif diff.seconds > 3600:
        hours = diff.seconds // 3600
        return f"{hours} hours ago"
    elif diff.seconds > 60:
        minutes = diff.seconds // 60
        return f"{minutes} minutes ago"
    else:
        return "Just now"

def get_status_color(status: str) -> str:
    """Get color for status display"""
    colors = {
        'pending': '#FFA500',  # Orange
        'approved': '#28a745',  # Green
        'rejected': '#dc3545',  # Red
        'edited': '#17a2b8'     # Blue
    }
    return colors.get(status, '#6c757d')  # Default gray

def get_user_name() -> str:
    """Get current user name"""
    # Try to get from Databricks context first
    try:
        # In Databricks, this might be available
        user = os.environ.get('DATABRICKS_USER')
        if user:
            return user
    except:
        pass
    
    # Try to get from system
    try:
        return getpass.getuser()
    except:
        return "Unknown User"

def validate_metadata_value(value: str, metadata_type: str) -> tuple[bool, str]:
    """Validate metadata value"""
    if not value or not value.strip():
        return False, f"{metadata_type.title()} cannot be empty"
    
    if len(value) > 5000:
        return False, f"{metadata_type.title()} is too long (max 5000 characters)"
    
    if metadata_type == 'comment':
        # Comments should be descriptive
        if len(value.split()) < 3:
            return False, "Comments should be more descriptive (at least 3 words)"
    
    return True, ""

def create_sample_data() -> pd.DataFrame:
    """Create sample data for testing"""
    import random
    from datetime import timedelta
    
    tables = ['customers', 'orders', 'products', 'inventory', 'transactions']
    columns = {
        'customers': ['customer_id', 'name', 'email', 'phone', 'address'],
        'orders': ['order_id', 'customer_id', 'order_date', 'total_amount', 'status'],
        'products': ['product_id', 'name', 'description', 'price', 'category'],
        'inventory': ['product_id', 'quantity', 'location', 'last_updated'],
        'transactions': ['transaction_id', 'order_id', 'amount', 'payment_method', 'timestamp']
    }
    
    metadata_types = ['comment', 'description']
    statuses = ['pending', 'approved', 'rejected', 'edited']
    
    data = []
    for i in range(50):
        table = random.choice(tables)
        column = random.choice(columns[table]) if random.random() > 0.2 else None
        metadata_type = random.choice(metadata_types)
        status = random.choice(statuses)
        
        # Generate realistic metadata
        if metadata_type == 'comment':
            if column:
                generated_value = f"This column stores {column.replace('_', ' ')} information for the {table} table"
            else:
                generated_value = f"The {table} table contains core business data related to {table}"
        else:
            if column:
                generated_value = f"Detailed description of {column} field in {table}"
            else:
                generated_value = f"Comprehensive description of the {table} table structure and purpose"
        
        created_at = datetime.now() - timedelta(days=random.randint(1, 30))
        
        data.append({
            'catalog_name': 'main',
            'schema_name': 'default',
            'table_name': table,
            'column_name': column,
            'metadata_type': metadata_type,
            'original_value': None if random.random() > 0.3 else f"Original {metadata_type} for {table}",
            'generated_value': generated_value,
            'current_value': generated_value,
            'status': status,
            'created_at': created_at,
            'updated_at': created_at + timedelta(hours=random.randint(1, 24)),
            'created_by': random.choice(['system', 'data_team', 'ai_generator']),
            'reviewed_by': random.choice(['john.doe', 'jane.smith', 'data.admin']) if status != 'pending' else None,
            'reviewed_at': created_at + timedelta(hours=random.randint(1, 48)) if status != 'pending' else None
        })
    
    return pd.DataFrame(data)

def export_to_csv(df: pd.DataFrame, filename: str) -> str:
    """Export DataFrame to CSV and return download link"""
    csv = df.to_csv(index=False)
    return csv

def parse_bulk_upload_csv(uploaded_file) -> tuple[bool, pd.DataFrame, str]:
    """Parse and validate bulk upload CSV"""
    try:
        df = pd.read_csv(uploaded_file)
        
        # Required columns
        required_columns = ['catalog_name', 'schema_name', 'table_name', 'metadata_type', 'generated_value']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return False, pd.DataFrame(), f"Missing required columns: {missing_columns}"
        
        # Validate data types and values
        errors = []
        
        # Check for empty required fields
        for col in required_columns:
            if df[col].isnull().any():
                errors.append(f"Column '{col}' contains empty values")
        
        # Validate metadata_type values
        valid_types = ['comment', 'description']
        invalid_types = df[~df['metadata_type'].isin(valid_types)]['metadata_type'].unique()
        if len(invalid_types) > 0:
            errors.append(f"Invalid metadata_type values: {invalid_types}. Must be one of: {valid_types}")
        
        if errors:
            return False, df, "; ".join(errors)
        
        return True, df, "Validation successful"
    
    except Exception as e:
        return False, pd.DataFrame(), f"Error reading CSV: {str(e)}"

def get_table_summary(df: pd.DataFrame) -> dict:
    """Get summary statistics for a DataFrame"""
    if df.empty:
        return {}
    
    summary = {
        'total_items': len(df),
        'unique_tables': df['table_name'].nunique() if 'table_name' in df.columns else 0,
        'status_distribution': df['status'].value_counts().to_dict() if 'status' in df.columns else {},
        'metadata_types': df['metadata_type'].value_counts().to_dict() if 'metadata_type' in df.columns else {},
        'recent_activity': df.sort_values('updated_at', ascending=False).head(5) if 'updated_at' in df.columns else pd.DataFrame()
    }
    
    return summary

def highlight_changes(old_text: str, new_text: str) -> str:
    """Highlight changes between two text strings"""
    if old_text == new_text:
        return new_text
    
    # Simple highlighting - in a real app you might want more sophisticated diff
    return f"~~{old_text}~~ → **{new_text}**"

def sanitize_input(text: str) -> str:
    """Sanitize user input"""
    if not text:
        return ""
    
    # Remove potentially dangerous characters
    dangerous_chars = ['<', '>', '&', '"', "'", '\\', ';']
    for char in dangerous_chars:
        text = text.replace(char, '')
    
    return text.strip()

def get_databricks_context():
    """Get Databricks context information"""
    context = {
        'workspace_url': os.environ.get('DATABRICKS_HOST', 'Unknown'),
        'user': os.environ.get('DATABRICKS_USER', 'Unknown'),
        'cluster_id': os.environ.get('DATABRICKS_CLUSTER_ID', 'Unknown'),
        'is_databricks': 'DATABRICKS_RUNTIME_VERSION' in os.environ
    }
    return context
