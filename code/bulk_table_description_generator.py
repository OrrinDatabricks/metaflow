# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk Table Description Generator
# MAGIC
# MAGIC This notebook generates AI-powered descriptions for database tables and saves them to the metadata tracking database.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - OpenAI API key configured
# MAGIC - Database connection configured
# MAGIC - Target catalog and schema specified
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Scan specified catalog/schema for tables
# MAGIC 2. Analyze table structure and sample data
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

# COMMAND ----------

# Create widgets for configuration
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "default", "Schema Name") 
dbutils.widgets.text("table_filter", "", "Table Name Filter (optional, comma-separated)")
dbutils.widgets.text("openai_api_key", "", "OpenAI API Key")
dbutils.widgets.dropdown("openai_model", "gpt-4", ["gpt-4", "gpt-3.5-turbo"], "OpenAI Model")
dbutils.widgets.text("max_tables", "20", "Max Tables to Process")
dbutils.widgets.dropdown("include_schema_info", "Yes", ["Yes", "No"], "Include Column Schema in Prompts")
dbutils.widgets.dropdown("include_sample_data", "Yes", ["Yes", "No"], "Include Sample Row Data")

# Get widget values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_filter = dbutils.widgets.get("table_filter")
openai_api_key = dbutils.widgets.get("openai_api_key")
openai_model = dbutils.widgets.get("openai_model")
max_tables = int(dbutils.widgets.get("max_tables"))
include_schema_info = dbutils.widgets.get("include_schema_info") == "Yes"
include_sample_data = dbutils.widgets.get("include_sample_data") == "Yes"

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
print(f"  Include Schema Info: {include_schema_info}")
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

def get_table_schema(catalog: str, schema: str, table: str) -> pd.DataFrame:
    """Get detailed table schema information"""
    try:
        query = f"DESCRIBE EXTENDED {catalog}.{schema}.{table}"
        desc_df = spark.sql(query)
        schema_df = desc_df.toPandas()
        
        # Filter to just the column information
        column_info = schema_df[
            ~schema_df['col_name'].str.startswith('#') &
            (schema_df['col_name'] != '') &
            ~schema_df['col_name'].isin(['', '# Partition Information', '# col_name', '# Detailed Table Information'])
        ].copy()
        
        return column_info
    except Exception as e:
        logger.error(f"Failed to get schema for {table}: {e}")
        return pd.DataFrame()

def get_table_stats(catalog: str, schema: str, table: str) -> Dict:
    """Get table statistics"""
    try:
        # Get row count
        count_query = f"SELECT COUNT(*) as row_count FROM {catalog}.{schema}.{table}"
        count_result = spark.sql(count_query).collect()
        row_count = count_result[0].row_count if count_result else 0
        
        # Get table properties
        props_query = f"SHOW TBLPROPERTIES {catalog}.{schema}.{table}"
        try:
            props_df = spark.sql(props_query)
            properties = {row.key: row.value for row in props_df.collect()}
        except:
            properties = {}
        
        return {
            "row_count": row_count,
            "properties": properties
        }
    except Exception as e:
        logger.warning(f"Failed to get stats for {table}: {e}")
        return {"row_count": 0, "properties": {}}

def sample_table_data(catalog: str, schema: str, table: str, limit: int = 3) -> List[Dict]:
    """Sample a few rows from the table"""
    try:
        query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}"
        result = spark.sql(query)
        
        # Convert to list of dictionaries
        rows = []
        for row in result.collect():
            row_dict = row.asDict()
            # Convert to strings and truncate long values
            for key, value in row_dict.items():
                if value is not None:
                    str_value = str(value)
                    if len(str_value) > 50:
                        row_dict[key] = str_value[:47] + "..."
                    else:
                        row_dict[key] = str_value
                else:
                    row_dict[key] = "NULL"
            rows.append(row_dict)
        
        return rows
    except Exception as e:
        logger.warning(f"Failed to sample data from {table}: {e}")
        return []

def generate_table_description(table_name: str, schema_info: Optional[pd.DataFrame] = None,
                              table_stats: Optional[Dict] = None, 
                              sample_data: Optional[List[Dict]] = None) -> Tuple[str, str, Dict]:
    """Generate AI description for a table"""
    
    # Build the prompt
    prompt = f"""Generate a concise, business-friendly description for this database table:

Table: {table_name}"""
    
    if table_stats and table_stats.get('row_count', 0) > 0:
        prompt += f"\nApproximate Rows: {table_stats['row_count']:,}"
    
    if schema_info is not None and include_schema_info and not schema_info.empty:
        prompt += f"\nColumns ({len(schema_info)}):"
        for _, row in schema_info.head(10).iterrows():  # Limit to first 10 columns
            col_name = row['col_name']
            data_type = row['data_type']
            prompt += f"\n  - {col_name} ({data_type})"
        
        if len(schema_info) > 10:
            prompt += f"\n  ... and {len(schema_info) - 10} more columns"
    
    if sample_data and include_sample_data:
        prompt += f"\nSample Data:"
        for i, row in enumerate(sample_data[:2]):  # Show max 2 sample rows
            prompt += f"\nRow {i+1}: {json.dumps(row, default=str)}"
    
    prompt += """

Requirements:
- Write 2-3 sentences maximum
- Focus on business purpose and what data this table contains
- Use clear, non-technical language that business users can understand
- Describe what business process or entity this table represents
- Don't mention technical details like data types or row counts
- Be specific about the table's role in the business

Example: "Contains customer profile information including contact details and account preferences. Used by marketing and customer service teams to manage customer relationships and communications."
"""

    try:
        # Call OpenAI API
        response = openai.ChatCompletion.create(
            model=openai_model,
            messages=[
                {"role": "system", "content": "You are a data analyst writing business-friendly table descriptions for a data catalog. Focus on business value and purpose."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=200,
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
            "completion_tokens": response.usage.completion_tokens,
            "column_count": len(schema_info) if schema_info is not None else 0,
            "row_count": table_stats.get('row_count', 0) if table_stats else 0
        }
        
        return description, prompt, generation_metadata
        
    except Exception as e:
        logger.error(f"Failed to generate description for {table_name}: {e}")
        return f"Auto-generated description for {table_name} table", prompt, {"error": str(e)}

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
successful_generations = 0
failed_generations = 0
processing_stats = []

# COMMAND ----------

# Process each table
for table_idx, table_name in enumerate(tables):
    print(f"\n{'='*60}")
    print(f"Processing table {table_idx + 1}/{len(tables)}: {table_name}")
    print(f"{'='*60}")
    
    table_stats = {
        "table_name": table_name,
        "start_time": datetime.now(),
        "status": "processing"
    }
    
    try:
        # Get table schema
        schema_info = None
        if include_schema_info:
            schema_info = get_table_schema(catalog_name, schema_name, table_name)
            print(f"  Schema: {len(schema_info)} columns")
        
        # Get table statistics
        stats = get_table_stats(catalog_name, schema_name, table_name)
        print(f"  Rows: {stats['row_count']:,}")
        
        # Sample data
        sample_data = None
        if include_sample_data:
            sample_data = sample_table_data(catalog_name, schema_name, table_name)
            print(f"  Sample: {len(sample_data)} rows")
        
        # Generate description
        print("  Generating description...")
        description, prompt, metadata = generate_table_description(
            table_name, schema_info, stats, sample_data
        )
        
        # Save to database
        success = db.insert_metadata_item(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            column_name=None,  # This is for table-level metadata
            metadata_type='description',
            original_value=None,  # No existing description
            generated_value=description,
            created_by='ai_table_generator',
            generation_method=f'openai_{openai_model}',
            generation_prompt=prompt,
            generation_metadata=json.dumps(metadata),
            data_sample=json.dumps(sample_data) if sample_data else None
        )
        
        if success:
            successful_generations += 1
            table_stats["status"] = "success"
            print(f"  ✅ Generated: {description[:80]}...")
        else:
            failed_generations += 1
            table_stats["status"] = "failed"
            print(f"  ❌ Failed to save to database")
        
        # Rate limiting to avoid API limits
        time.sleep(1)
        
    except Exception as e:
        failed_generations += 1
        table_stats["status"] = "error"
        table_stats["error"] = str(e)
        print(f"  ❌ Error: {e}")
    
    table_stats["end_time"] = datetime.now()
    table_stats["duration"] = (table_stats["end_time"] - table_stats["start_time"]).total_seconds()
    processing_stats.append(table_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

print("\n" + "="*80)
print("BULK TABLE DESCRIPTION GENERATION COMPLETE")
print("="*80)

total_tables = len(tables)
print(f"\nOverall Statistics:")
print(f"  Total Tables Processed: {total_tables}")
print(f"  Successful Generations: {successful_generations}")
print(f"  Failed Generations: {failed_generations}")
print(f"  Success Rate: {(successful_generations/total_tables*100):.1f}%" if total_tables > 0 else "N/A")

print(f"\nTable-by-Table Results:")
for stats in processing_stats:
    status_icon = "✅" if stats["status"] == "success" else "❌"
    print(f"  {status_icon} {stats['table_name']}: {stats['status']} ({stats['duration']:.1f}s)")
    if "error" in stats:
        print(f"      Error: {stats['error']}")

# Create results DataFrame for display
results_df = pd.DataFrame(processing_stats)
if not results_df.empty:
    display(results_df[['table_name', 'status', 'duration']])

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
    ).head(successful_generations)
    
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
        
        if total_cost > 0:
            print(f"\n💰 Cost Estimation:")
            print(f"  Estimated Total Cost: ${total_cost:.4f}")
            print(f"  Average Cost per Table: ${total_cost/successful_generations:.6f}")
            print(f"  Total Tokens Used: {total_tokens:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Analysis

# COMMAND ----------

# Analyze the generated descriptions
if successful_generations > 0:
    recent_items = db.get_metadata_items(
        catalog_name=catalog_name,
        schema_name=schema_name
    ).head(successful_generations)
    
    if not recent_items.empty:
        print(f"\n📊 Quality Analysis:")
        
        # Description length analysis
        desc_lengths = recent_items['generated_value'].str.len()
        print(f"  Average Description Length: {desc_lengths.mean():.0f} characters")
        print(f"  Description Length Range: {desc_lengths.min()}-{desc_lengths.max()} characters")
        
        # Show a few sample descriptions
        print(f"\n📝 Sample Generated Descriptions:")
        for i, (_, item) in enumerate(recent_items.head(3).iterrows()):
            print(f"  {i+1}. {item['table_name']}: {item['generated_value']}")
        
        # Token usage analysis
        token_data = []
        for _, item in recent_items.iterrows():
            if item['generation_metadata']:
                try:
                    metadata = json.loads(item['generation_metadata'])
                    if 'tokens_used' in metadata:
                        token_data.append(metadata['tokens_used'])
                except:
                    pass
        
        if token_data:
            avg_tokens = sum(token_data) / len(token_data)
            print(f"\n🔢 Token Usage:")
            print(f"  Average Tokens per Table: {avg_tokens:.0f}")
            print(f"  Token Range: {min(token_data)}-{max(token_data)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Review Generated Descriptions**: Open the Streamlit app to review the generated table descriptions
# MAGIC 2. **Approve/Edit**: Business users can approve, edit, or reject the descriptions
# MAGIC 3. **Apply to Catalog**: Use the importer notebook to apply approved descriptions to the actual data catalog
# MAGIC 4. **Generate Column Descriptions**: Run the column description generator for detailed column metadata
# MAGIC
# MAGIC **Streamlit App**: Access the metadata review interface to manage the generated descriptions
# MAGIC
# MAGIC **Next Notebook**: Run `bulk_table_description_importer` after approving descriptions to apply them to your data catalog
