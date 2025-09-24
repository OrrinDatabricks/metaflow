"""
Generate sample data for testing the Metadata Enrichment Tracker
"""
import pandas as pd
import random
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.database import get_db_manager
from config import Config
from app.utils import get_user_name

def generate_sample_metadata():
    """Generate sample metadata items"""
    
    # Sample tables and columns
    schema_data = {
        'customers': {
            'description': 'Core customer information and profile data',
            'columns': {
                'customer_id': 'Unique identifier for each customer record',
                'first_name': 'Customer first name as provided during registration',
                'last_name': 'Customer last name or surname',
                'email': 'Primary email address for customer communications',
                'phone': 'Primary phone number for customer contact',
                'address': 'Complete mailing address for the customer',
                'registration_date': 'Date when the customer account was created',
                'status': 'Current status of the customer account (active, inactive, suspended)'
            }
        },
        'orders': {
            'description': 'Transaction records for all customer purchases',
            'columns': {
                'order_id': 'Unique identifier for each order transaction',
                'customer_id': 'Foreign key reference to the customers table',
                'order_date': 'Timestamp when the order was placed',
                'total_amount': 'Total monetary value of the order including taxes',
                'status': 'Current processing status of the order',
                'shipping_address': 'Delivery address for the order',
                'payment_method': 'Method used for payment (credit card, PayPal, etc.)'
            }
        },
        'products': {
            'description': 'Catalog of all available products and their details',
            'columns': {
                'product_id': 'Unique identifier for each product in the catalog',
                'name': 'Display name of the product',
                'description': 'Detailed description of the product features',
                'price': 'Current selling price of the product',
                'category': 'Product category classification',
                'brand': 'Manufacturer or brand name',
                'sku': 'Stock keeping unit identifier for inventory management'
            }
        },
        'inventory': {
            'description': 'Stock levels and warehouse information for products',
            'columns': {
                'product_id': 'Foreign key reference to the products table',
                'quantity': 'Current available quantity in stock',
                'location': 'Warehouse or storage location identifier',
                'last_updated': 'Timestamp of the most recent inventory update',
                'reorder_level': 'Minimum quantity threshold for reordering',
                'supplier_id': 'Identifier for the product supplier'
            }
        },
        'transactions': {
            'description': 'Financial transaction records and payment processing data',
            'columns': {
                'transaction_id': 'Unique identifier for each financial transaction',
                'order_id': 'Foreign key reference to the orders table',
                'amount': 'Transaction amount in base currency',
                'payment_method': 'Payment processing method used',
                'timestamp': 'Exact time when the transaction was processed',
                'status': 'Transaction processing status (pending, completed, failed)',
                'gateway_response': 'Response code from the payment gateway'
            }
        }
    }
    
    data = []
    statuses = ['pending', 'approved', 'rejected', 'edited']
    metadata_types = ['comment', 'description']
    users = ['ai_generator', 'data_team', 'system_import']
    reviewers = ['john.doe@company.com', 'jane.smith@company.com', 'data.admin@company.com']
    
    # Generate table-level metadata
    for table_name, table_info in schema_data.items():
        for metadata_type in metadata_types:
            status = random.choice(statuses)
            created_at = datetime.now() - timedelta(days=random.randint(1, 30))
            
            if metadata_type == 'description':
                generated_value = table_info['description']
            else:
                generated_value = f"Primary table for {table_name.replace('_', ' ')} data management"
            
        data.append({
            'catalog_name': 'main',
            'schema_name': 'default',
            'table_name': table_name,
            'column_name': None,
            'metadata_type': metadata_type,
            'original_value': None if random.random() > 0.3 else f"Old {metadata_type} for {table_name}",
            'generated_value': generated_value,
            'current_value': generated_value,
            'status': status,
            'created_at': created_at,
            'updated_at': created_at + timedelta(hours=random.randint(1, 24)) if status != 'pending' else created_at,
            'created_by': random.choice(users),
            'reviewed_by': random.choice(reviewers) if status != 'pending' else None,
            'reviewed_at': created_at + timedelta(hours=random.randint(1, 48)) if status != 'pending' else None
        })
        
        # Generate column-level metadata
        for column_name, column_desc in table_info['columns'].items():
            for metadata_type in metadata_types:
                status = random.choice(statuses)
                created_at = datetime.now() - timedelta(days=random.randint(1, 30))
                
                if metadata_type == 'description':
                    generated_value = column_desc
                else:
                    generated_value = f"Column storing {column_name.replace('_', ' ')} information"
                
                # Add some variation to the generated values
                if random.random() > 0.7:
                    generated_value += f" with data validation and business rules applied"
                
                data.append({
                    'catalog_name': 'main',
                    'schema_name': 'default',
                    'table_name': table_name,
                    'column_name': column_name,
                    'metadata_type': metadata_type,
                    'original_value': None if random.random() > 0.4 else f"Previous {metadata_type}",
                    'generated_value': generated_value,
                    'current_value': generated_value,
                    'status': status,
                    'created_at': created_at,
                    'updated_at': created_at + timedelta(hours=random.randint(1, 24)) if status != 'pending' else created_at,
                    'created_by': random.choice(users),
                    'reviewed_by': random.choice(reviewers) if status != 'pending' else None,
                    'reviewed_at': created_at + timedelta(hours=random.randint(1, 48)) if status != 'pending' else None
                })
    
    return data

def insert_sample_data():
    """Insert sample data into the database"""
    db = get_db_manager()
    
    # Create tables if they don't exist
    if not db.create_tables():
        print("Failed to create database tables")
        return False
    
    # Generate sample data
    sample_data = generate_sample_metadata()
    
    print(f"Inserting {len(sample_data)} sample records...")
    
    success_count = 0
    for item in sample_data:
        if db.insert_metadata_item(
            catalog_name=item['catalog_name'],
            schema_name=item['schema_name'],
            table_name=item['table_name'],
            column_name=item['column_name'],
            metadata_type=item['metadata_type'],
            original_value=item['original_value'],
            generated_value=item['generated_value'],
            created_by=item['created_by'],
            generation_method='sample_generator'
        ):
            success_count += 1
    
    print(f"Successfully inserted {success_count} out of {len(sample_data)} records")
    return success_count == len(sample_data)

def create_sample_csv():
    """Create a sample CSV file for bulk upload testing"""
    sample_data = generate_sample_metadata()
    
    # Convert to DataFrame and save
    df = pd.DataFrame(sample_data)
    
    # Select only the columns needed for bulk upload
    upload_df = df[['catalog_name', 'schema_name', 'table_name', 'column_name', 'metadata_type', 'original_value', 'generated_value']].copy()
    
    filename = f"sample_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    upload_df.to_csv(filename, index=False)
    
    print(f"Sample CSV file created: {filename}")
    return filename

if __name__ == "__main__":
    print("Metadata Enrichment Tracker - Sample Data Generator")
    print("=" * 50)
    
    choice = input("Choose an option:\n1. Insert sample data into database\n2. Create sample CSV file\n3. Both\nEnter choice (1-3): ")
    
    if choice in ['1', '3']:
        print("\nInserting sample data into database...")
        if insert_sample_data():
            print("✅ Sample data inserted successfully!")
        else:
            print("❌ Failed to insert sample data")
    
    if choice in ['2', '3']:
        print("\nCreating sample CSV file...")
        filename = create_sample_csv()
        print(f"✅ Sample CSV created: {filename}")
    
    print("\nDone!")
