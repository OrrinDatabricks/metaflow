# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk Column Description Generator
# MAGIC
# MAGIC This notebook generates AI-powered descriptions for database columns and saves them to the metadata tracking database.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - OpenAI API key configured
# MAGIC - Database connection configured
# MAGIC - Target catalog and schema specified
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Scan specified catalog/schema for tables and columns
# MAGIC 2. Sample data from each column
# MAGIC 3. Generate descriptions using AI
# MAGIC 4. Save to metadata tracking database
# MAGIC 5. Business users can review and approve via Streamlit app

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Import required libraries
import pandas as pd
import json
import openai
from typing import Dict, List, Optional, Tuple
import time
import logging
from datetime import datetime
import os
import sys

# Add the parent directory to path to import our modules
sys.path.append('/Workspace/Repos/your-repo/MetaFlow')  # Update this path

from database.database import get_db_manager
from config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration
# MAGIC Configure the parameters for bulk generation

# COMMAND ----------

# Create widgets for configuration
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "default", "Schema Name") 
dbutils.widgets.text("table_filter", "", "Table Name Filter (optional, comma-separated)")
dbutils.widgets.text("openai_api_key", "", "OpenAI API Key")
dbutils.widgets.dropdown("openai_model", "gpt-4", ["gpt-4", "gpt-3.5-turbo"], "OpenAI Model")
dbutils.widgets.text("max_tables", "10", "Max Tables to Process")
dbutils.widgets.text("max_columns_per_table", "50", "Max Columns per Table")
dbutils.widgets.dropdown("sample_data", "Yes", ["Yes", "No"], "Include Sample Data in Prompts")

# Get widget values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_filter = dbutils.widgets.get("table_filter")
openai_api_key = dbutils.widgets.get("openai_api_key")
openai_model = dbutils.widgets.get("openai_model")
max_tables = int(dbutils.widgets.get("max_tables"))
max_columns_per_table = int(dbutils.widgets.get("max_columns_per_table"))
include_sample_data = dbutils.widgets.get("sample_data") == "Yes"

# Validate inputs
if not openai_api_key:
    raise ValueError("OpenAI API key is required")

# Configure OpenAI
openai.api_key = openai_api_key

print(f"Configuration:")
print(f"  Catalog: {catalog_name}")
print(f"  Schema: {schema_name}")
print(f"  Table Filter: {table_filter or 'None'}")
print(f"  Model: {openai_model}")
print(f"  Max Tables: {max_tables}")
print(f"  Max Columns per Table: {max_columns_per_table}")
print(f"  Include Sample Data: {include_sample_data}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_table_list(catalog: str, schema: str, table_filter: Optional[str] = None) -> List[str]:
    """Get list of tables in the specified catalog/schema"""
    try:
        query = f"SHOW TABLES IN {catalog}.{schema}"
        tables_df = spark.sql(query)
        
        table_names = [row.tableName for row in tables_df.collect()]
        
        # Apply filter if provided
        if table_filter:
            filter_list = [t.strip() for t in table_filter.split(',')]
            table_names = [t for t in table_names if any(f in t for f in filter_list)]
        
        return table_names[:max_tables]
    except Exception as e:
        logger.error(f"Failed to get table list: {e}")
        return []

def get_column_info(catalog: str, schema: str, table: str) -> pd.DataFrame:
    """Get column information for a table"""
    try:
        query = f"DESCRIBE {catalog}.{schema}.{table}"
        desc_df = spark.sql(query)
        
        # Convert to pandas and clean up
        columns_df = desc_df.toPandas()
        
        # Filter out partition info and other metadata
        columns_df = columns_df[
            ~columns_df['col_name'].str.startswith('#') &
            (columns_df['col_name'] != '') &
            ~columns_df['col_name'].isin(['', '# Partition Information', '# col_name'])
        ].copy()
        
        # Limit columns if specified
        if len(columns_df) > max_columns_per_table:
            columns_df = columns_df.head(max_columns_per_table)
        
        return columns_df
    except Exception as e:
        logger.error(f"Failed to get column info for {table}: {e}")
        return pd.DataFrame()

def sample_column_data(catalog: str, schema: str, table: str, column: str, sample_size: int = 5) -> List[str]:
    """Sample data from a column"""
    try:
        query = f"""
        SELECT DISTINCT {column}
        FROM {catalog}.{schema}.{table}
        WHERE {column} IS NOT NULL
        LIMIT {sample_size}
        """
        
        result = spark.sql(query)
        samples = [str(row[0]) for row in result.collect()]
        return samples
    except Exception as e:
        logger.warning(f"Failed to sample data from {table}.{column}: {e}")
        return []

def generate_column_description(table_name: str, column_name: str, data_type: str, 
                               sample_data: Optional[List[str]] = None) -> Tuple[str, str, Dict]:
    """Generate AI description for a column"""
    
    # Build the prompt
    prompt = f"""Generate a concise, business-friendly description for this database column:

Table: {table_name}
Column: {column_name}
Data Type: {data_type}"""
    
    if sample_data and include_sample_data:
        prompt += f"\nSample Values: {', '.join(sample_data[:3])}"
    
    prompt += """

Requirements:
- Write 1-2 sentences maximum
- Focus on business purpose, not technical details
- Use clear, non-technical language
- Don't mention data types or technical constraints
- Be specific about what the column represents

Example: "Customer's primary email address used for communications and account notifications."
"""

    try:
        # Call OpenAI API
        response = openai.ChatCompletion.create(
            model=openai_model,
            messages=[
                {"role": "system", "content": "You are a data analyst writing business-friendly column descriptions for a data catalog."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150,
            temperature=0.3
        )
        
        description = response.choices[0].message.content.strip()
        
        # Metadata about the generation
        generation_metadata = {
            "model": openai_model,
            "tokens_used": response.usage.total_tokens,
            "cost_estimate": response.usage.total_tokens * 0.00003,  # Rough estimate
            "timestamp": datetime.now().isoformat(),
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens
        }
        
        return description, prompt, generation_metadata
        
    except Exception as e:
        logger.error(f"Failed to generate description for {column_name}: {e}")
        return f"Auto-generated description for {column_name}", prompt, {"error": str(e)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing

# COMMAND ----------

# Initialize database connection
db = get_db_manager()

# Ensure tables exist
if not db.create_tables():
    raise Exception("Failed to create database tables")

# Get list of tables to process
print("Getting table list...")
tables = get_table_list(catalog_name, schema_name, table_filter)
print(f"Found {len(tables)} tables to process: {tables}")

# Track processing stats
total_columns = 0
successful_generations = 0
failed_generations = 0
processing_stats = []

# COMMAND ----------

# Process each table
for table_idx, table_name in enumerate(tables):
    print(f"\n{'='*60}")
    print(f"Processing table {table_idx + 1}/{len(tables)}: {table_name}")
    print(f"{'='*60}")
    
    # Get column information
    columns_df = get_column_info(catalog_name, schema_name, table_name)
    
    if columns_df.empty:
        print(f"  No columns found for {table_name}, skipping...")
        continue
    
    print(f"  Found {len(columns_df)} columns")
    
    table_stats = {
        "table_name": table_name,
        "total_columns": len(columns_df),
        "successful": 0,
        "failed": 0,
        "start_time": datetime.now()
    }
    
    # Process each column
    for idx, row in columns_df.iterrows():
        column_name = row['col_name']
        data_type = row['data_type']
        
        total_columns += 1
        
        print(f"    Processing column {idx + 1}/{len(columns_df)}: {column_name} ({data_type})")
        
        try:
            # Sample data if requested
            sample_data = None
            if include_sample_data:
                sample_data = sample_column_data(catalog_name, schema_name, table_name, column_name)
            
            # Generate description
            description, prompt, metadata = generate_column_description(
                table_name, column_name, data_type, sample_data
            )
            
            # Save to database
            success = db.insert_metadata_item(
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                metadata_type='description',
                original_value=None,  # No existing description
                generated_value=description,
                created_by='ai_bulk_generator',
                generation_method=f'openai_{openai_model}',
                generation_prompt=prompt,
                generation_metadata=json.dumps(metadata),
                data_sample=json.dumps(sample_data) if sample_data else None
            )
            
            if success:
                successful_generations += 1
                table_stats["successful"] += 1
                print(f"      ✅ Generated: {description[:50]}...")
            else:
                failed_generations += 1
                table_stats["failed"] += 1
                print(f"      ❌ Failed to save to database")
            
            # Rate limiting to avoid API limits
            time.sleep(0.5)
            
        except Exception as e:
            failed_generations += 1
            table_stats["failed"] += 1
            print(f"      ❌ Error: {e}")
    
    table_stats["end_time"] = datetime.now()
    table_stats["duration"] = (table_stats["end_time"] - table_stats["start_time"]).total_seconds()
    processing_stats.append(table_stats)
    
    print(f"  Table {table_name} completed: {table_stats['successful']} successful, {table_stats['failed']} failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

print("\n" + "="*80)
print("BULK COLUMN DESCRIPTION GENERATION COMPLETE")
print("="*80)

print(f"\nOverall Statistics:")
print(f"  Total Columns Processed: {total_columns}")
print(f"  Successful Generations: {successful_generations}")
print(f"  Failed Generations: {failed_generations}")
print(f"  Success Rate: {(successful_generations/total_columns*100):.1f}%" if total_columns > 0 else "N/A")

print(f"\nTable-by-Table Results:")
for stats in processing_stats:
    print(f"  {stats['table_name']}: {stats['successful']}/{stats['total_columns']} successful ({stats['duration']:.1f}s)")

# Create results DataFrame for display
results_df = pd.DataFrame(processing_stats)
if not results_df.empty:
    results_df['success_rate'] = (results_df['successful'] / results_df['total_columns'] * 100).round(1)
    display(results_df[['table_name', 'total_columns', 'successful', 'failed', 'success_rate', 'duration']])

print(f"\n✅ Generated descriptions have been saved to the database with status 'pending'")
print(f"📊 Business users can now review and approve them in the Streamlit app")
print(f"🔗 Database schema: {Config.DB_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Estimation

# COMMAND ----------

# Calculate estimated costs
if successful_generations > 0:
    # Get a sample of generation metadata to estimate costs
    sample_items = db.get_metadata_items(
        catalog_name=catalog_name,
        schema_name=schema_name
    ).head(10)
    
    if not sample_items.empty:
        total_cost = 0
        total_tokens = 0
        
        for _, item in sample_items.iterrows():
            if item['generation_metadata']:
                try:
                    metadata = json.loads(item['generation_metadata'])
                    if 'cost_estimate' in metadata:
                        total_cost += metadata['cost_estimate']
                    if 'tokens_used' in metadata:
                        total_tokens += metadata['tokens_used']
                except:
                    pass
        
        # Extrapolate to all successful generations
        if total_cost > 0:
            avg_cost = total_cost / len(sample_items)
            estimated_total_cost = avg_cost * successful_generations
            
            print(f"\n💰 Cost Estimation:")
            print(f"  Estimated Total Cost: ${estimated_total_cost:.4f}")
            print(f"  Average Cost per Column: ${avg_cost:.6f}")
            print(f"  Total Tokens Used: ~{total_tokens * successful_generations // len(sample_items):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Review Generated Descriptions**: Open the Streamlit app to review the generated descriptions
# MAGIC 2. **Approve/Edit**: Business users can approve, edit, or reject the descriptions
# MAGIC 3. **Apply to Catalog**: Use the importer notebook to apply approved descriptions to the actual data catalog
# MAGIC 4. **Monitor Quality**: Track approval rates and feedback to improve future generations
# MAGIC
# MAGIC **Streamlit App**: Access the metadata review interface to manage the generated descriptions
# MAGIC
# MAGIC **Next Notebook**: Run `bulk_column_description_importer` after approving descriptions to apply them to your data catalog
