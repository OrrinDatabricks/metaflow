# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk Table Description Importer
# MAGIC
# MAGIC This notebook imports approved table descriptions from the metadata tracking database and applies them to the actual data catalog using ALTER TABLE statements.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Table descriptions have been generated and approved in the Streamlit app
# MAGIC - Appropriate permissions to alter table schemas
# MAGIC - Database connection configured
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Query approved table descriptions from tracking database
# MAGIC 2. Generate ALTER TABLE statements
# MAGIC 3. Apply descriptions to the data catalog
# MAGIC 4. Update status in tracking database

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Import required libraries
import pandas as pd
import json
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
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
dbutils.widgets.dropdown("status_filter", "approved", ["approved", "edited", "all_approved"], "Status Filter")
dbutils.widgets.dropdown("dry_run", "Yes", ["Yes", "No"], "Dry Run (Preview Only)")
dbutils.widgets.text("batch_size", "20", "Batch Size (tables per batch)")

# Get widget values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_filter = dbutils.widgets.get("table_filter")
status_filter = dbutils.widgets.get("status_filter")
dry_run = dbutils.widgets.get("dry_run") == "Yes"
batch_size = int(dbutils.widgets.get("batch_size"))

print(f"Configuration:")
print(f"  Catalog: {catalog_name}")
print(f"  Schema: {schema_name}")
print(f"  Table Filter: {table_filter or 'None'}")
print(f"  Status Filter: {status_filter}")
print(f"  Dry Run: {dry_run}")
print(f"  Batch Size: {batch_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_approved_table_descriptions(catalog: str, schema: str, status: str, table_filter: Optional[str] = None) -> pd.DataFrame:
    """Get approved table descriptions from the tracking database"""
    db = get_db_manager()
    
    # Build status list
    if status == "all_approved":
        status_list = ["approved", "edited"]
    else:
        status_list = [status]
    
    all_descriptions = []
    
    for status_value in status_list:
        descriptions = db.get_metadata_items(
            status=status_value,
            catalog_name=catalog,
            schema_name=schema
        )
        
        if not descriptions.empty:
            all_descriptions.append(descriptions)
    
    if not all_descriptions:
        return pd.DataFrame()
    
    # Combine all descriptions
    combined_df = pd.concat(all_descriptions, ignore_index=True)
    
    # Filter for table-level descriptions only (column_name is NULL)
    table_descriptions = combined_df[
        (combined_df['column_name'].isna() | (combined_df['column_name'] == '')) &
        (combined_df['metadata_type'] == 'description')
    ].copy()
    
    # Apply table filter if provided
    if table_filter:
        filter_list = [t.strip() for t in table_filter.split(',')]
        table_descriptions = table_descriptions[
            table_descriptions['table_name'].apply(
                lambda x: any(f in x for f in filter_list)
            )
        ]
    
    return table_descriptions

def escape_sql_string(text: str) -> str:
    """Escape single quotes in SQL strings"""
    if text is None:
        return "NULL"
    return text.replace("'", "''")

def generate_table_alter_statement(catalog: str, schema: str, table: str, description: str) -> str:
    """Generate ALTER TABLE statement to add table comment"""
    escaped_description = escape_sql_string(description)
    
    return f"""ALTER TABLE {catalog}.{schema}.{table} 
SET TBLPROPERTIES ('comment' = '{escaped_description}');"""

def apply_table_description(catalog: str, schema: str, table: str, description: str, dry_run: bool = True) -> Tuple[bool, str]:
    """Apply table description to the actual table"""
    alter_statement = generate_table_alter_statement(catalog, schema, table, description)
    
    if dry_run:
        return True, f"DRY RUN: Would execute - {alter_statement}"
    
    try:
        spark.sql(alter_statement)
        return True, f"Successfully applied description to table {catalog}.{schema}.{table}"
    except Exception as e:
        error_msg = f"Failed to apply description to table {catalog}.{schema}.{table}: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def verify_table_exists(catalog: str, schema: str, table: str) -> bool:
    """Verify that a table exists before trying to alter it"""
    try:
        spark.sql(f"DESCRIBE {catalog}.{schema}.{table}")
        return True
    except Exception:
        return False

def update_import_status(db, item_id: int, success: bool, message: str):
    """Update the status of imported items"""
    if not dry_run:
        # Add to approval history
        try:
            from database.database import ApprovalHistory
            session = db.Session()
            
            history = ApprovalHistory(
                metadata_item_id=item_id,
                action="imported" if success else "import_failed",
                old_value=None,
                new_value=None,
                comment=message,
                user_name="bulk_table_importer"
            )
            session.add(history)
            session.commit()
            session.close()
        except Exception as e:
            logger.warning(f"Failed to update import status: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing

# COMMAND ----------

# Initialize database connection
db = get_db_manager()

# Get approved table descriptions
print("Fetching approved table descriptions...")
descriptions_df = get_approved_table_descriptions(catalog_name, schema_name, status_filter, table_filter)

if descriptions_df.empty:
    print("❌ No approved table descriptions found matching the criteria.")
    print("Make sure you have:")
    print("  1. Generated table descriptions using the generator notebook")
    print("  2. Approved them in the Streamlit app")
    print("  3. Set the correct catalog/schema names")
    dbutils.notebook.exit("No descriptions to import")

print(f"✅ Found {len(descriptions_df)} approved table descriptions")

# Show summary of tables to be updated
print(f"📊 Tables to be updated:")
for _, row in descriptions_df.iterrows():
    table_full_name = f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}"
    description_preview = row['current_value'][:80] + "..." if len(row['current_value']) > 80 else row['current_value']
    print(f"  - {table_full_name}: {description_preview}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("\n🔍 Validating tables exist...")

validation_results = []
valid_tables = []

for _, row in descriptions_df.iterrows():
    table_exists = verify_table_exists(row['catalog_name'], row['schema_name'], row['table_name'])
    
    validation_result = {
        "catalog_name": row['catalog_name'],
        "schema_name": row['schema_name'],
        "table_name": row['table_name'],
        "exists": table_exists
    }
    validation_results.append(validation_result)
    
    if table_exists:
        valid_tables.append(row)
        print(f"  ✅ {row['catalog_name']}.{row['schema_name']}.{row['table_name']}")
    else:
        print(f"  ❌ {row['catalog_name']}.{row['schema_name']}.{row['table_name']} - TABLE NOT FOUND")

# Filter to only valid tables
if len(valid_tables) < len(descriptions_df):
    print(f"\n⚠️  {len(descriptions_df) - len(valid_tables)} tables were not found and will be skipped")
    descriptions_df = pd.DataFrame(valid_tables)

if descriptions_df.empty:
    print("❌ No valid tables found to update")
    dbutils.notebook.exit("No valid tables to import")

print(f"✅ {len(descriptions_df)} valid tables ready for update")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Changes

# COMMAND ----------

print("\n" + "="*80)
print("PREVIEW OF CHANGES TO BE APPLIED")
print("="*80)

# Show all ALTER statements that would be executed
print(f"\nALTER statements to be executed ({len(descriptions_df)} tables):")

for i, (_, row) in enumerate(descriptions_df.iterrows()):
    alter_stmt = generate_table_alter_statement(
        row['catalog_name'], 
        row['schema_name'], 
        row['table_name'], 
        row['current_value']
    )
    print(f"\n{i+1}. {alter_stmt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Changes

# COMMAND ----------

# Confirm execution if not in dry run mode
if not dry_run:
    print("⚠️  WARNING: This will modify your data catalog!")
    print("Make sure you have:")
    print("  1. Reviewed the preview above")
    print("  2. Appropriate permissions to alter tables")
    print("  3. Backup/rollback plan if needed")

print(f"\n{'='*80}")
if dry_run:
    print("DRY RUN MODE - NO CHANGES WILL BE APPLIED")
else:
    print("APPLYING TABLE DESCRIPTIONS TO DATA CATALOG")
print(f"{'='*80}")

# Track results
successful_imports = 0
failed_imports = 0
results = []

# Process in batches
total_batches = (len(descriptions_df) + batch_size - 1) // batch_size

for batch_num in range(total_batches):
    start_idx = batch_num * batch_size
    end_idx = min(start_idx + batch_size, len(descriptions_df))
    batch_df = descriptions_df.iloc[start_idx:end_idx]
    
    print(f"\n--- Batch {batch_num + 1}/{total_batches} ({len(batch_df)} tables) ---")
    
    for idx, (_, row) in enumerate(batch_df.iterrows()):
        full_table_name = f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}"
        
        print(f"  {start_idx + idx + 1}/{len(descriptions_df)}: {full_table_name}")
        
        # Apply the description
        success, message = apply_table_description(
            row['catalog_name'],
            row['schema_name'],
            row['table_name'],
            row['current_value'],
            dry_run
        )
        
        # Track results
        result = {
            "id": row['id'],
            "catalog_name": row['catalog_name'],
            "schema_name": row['schema_name'],
            "table_name": row['table_name'],
            "description": row['current_value'][:100] + "..." if len(row['current_value']) > 100 else row['current_value'],
            "success": success,
            "message": message
        }
        results.append(result)
        
        if success:
            successful_imports += 1
            print(f"    ✅ Success")
        else:
            failed_imports += 1
            print(f"    ❌ Failed: {message}")
        
        # Update status in tracking database
        update_import_status(db, row['id'], success, message)
    
    # Small delay between batches to avoid overwhelming the system
    if not dry_run and batch_num < total_batches - 1:
        import time
        time.sleep(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

print("\n" + "="*80)
if dry_run:
    print("DRY RUN COMPLETE - PREVIEW RESULTS")
else:
    print("BULK TABLE DESCRIPTION IMPORT COMPLETE")
print("="*80)

total_items = len(descriptions_df)
print(f"\nOverall Statistics:")
print(f"  Total Tables Processed: {total_items}")
print(f"  Successful Imports: {successful_imports}")
print(f"  Failed Imports: {failed_imports}")
print(f"  Success Rate: {(successful_imports/total_items*100):.1f}%" if total_items > 0 else "N/A")

# Create results DataFrame
results_df = pd.DataFrame(results)

# Show results by status
if not results_df.empty:
    print(f"\nDetailed Results:")
    for _, row in results_df.iterrows():
        status_icon = "✅" if row['success'] else "❌"
        print(f"  {status_icon} {row['catalog_name']}.{row['schema_name']}.{row['table_name']}")
        if not row['success']:
            print(f"      Error: {row['message']}")

# Show failed imports if any
failed_results = results_df[~results_df['success']]
if not failed_results.empty:
    print(f"\n❌ Failed Imports ({len(failed_results)}):")
    for _, row in failed_results.iterrows():
        print(f"  - {row['catalog_name']}.{row['schema_name']}.{row['table_name']}")
        print(f"    Error: {row['message']}")

# Display detailed results
if not results_df.empty:
    display(results_df[['catalog_name', 'schema_name', 'table_name', 'success', 'description']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

if not dry_run and successful_imports > 0:
    print(f"\n🔍 Verification - Checking Applied Descriptions")
    print("="*50)
    
    # Verify all successful imports
    for _, row in results_df[results_df['success']].iterrows():
        table_full_name = f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}"
        
        try:
            # Get the current table properties
            props_query = f"SHOW TBLPROPERTIES {table_full_name}"
            props_result = spark.sql(props_query).toPandas()
            
            # Look for the comment property
            comment_row = props_result[props_result['key'] == 'comment']
            
            if not comment_row.empty:
                current_comment = comment_row.iloc[0]['value']
                print(f"✅ {table_full_name}:")
                print(f"   Comment: {current_comment}")
            else:
                print(f"⚠️  No comment found for {table_full_name} (may not have been set)")
        
        except Exception as e:
            print(f"❌ Verification failed for {table_full_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Check

# COMMAND ----------

if not dry_run and successful_imports > 0:
    print(f"\n📊 Quality Check")
    print("="*30)
    
    # Check description lengths and quality
    successful_results = results_df[results_df['success']]
    
    if not successful_results.empty:
        desc_lengths = successful_results['description'].str.len()
        
        print(f"Description Statistics:")
        print(f"  Average Length: {desc_lengths.mean():.0f} characters")
        print(f"  Length Range: {desc_lengths.min()}-{desc_lengths.max()} characters")
        print(f"  Total Tables Updated: {len(successful_results)}")
        
        # Show sample of applied descriptions
        print(f"\nSample Applied Descriptions:")
        for i, (_, row) in enumerate(successful_results.head(3).iterrows()):
            print(f"  {i+1}. {row['catalog_name']}.{row['schema_name']}.{row['table_name']}:")
            print(f"     {row['description']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps

# COMMAND ----------

if dry_run:
    print("\n📋 Next Steps (DRY RUN):")
    print("1. Review the preview results above")
    print("2. If satisfied, set 'Dry Run' to 'No' and re-run this notebook")
    print("3. Monitor the import process for any failures")
    print("4. Verify descriptions in your data catalog")
else:
    print("\n📋 Next Steps:")
    print("1. ✅ Table descriptions have been applied to your data catalog")
    print("2. 🔍 Verify the changes in your data catalog interface")
    print("3. 📊 Check the Streamlit app for updated status")
    print("4. 🔄 Re-run for any failed imports after fixing issues")
    print("5. 📝 Consider running column description generator/importer for detailed metadata")
    
    if failed_imports > 0:
        print("\n⚠️  Some imports failed:")
        print("- Check error messages above")
        print("- Verify table names exist and are accessible")
        print("- Check permissions for ALTER TABLE operations")
        print("- Consider re-running for failed items only")

print(f"\n🔗 Metadata tracking database: {Config.DB_SCHEMA}")
print(f"📊 Review and manage metadata in the Streamlit app")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rollback Information
# MAGIC
# MAGIC If you need to rollback changes:
# MAGIC
# MAGIC ```sql
# MAGIC -- Remove table comment
# MAGIC ALTER TABLE catalog.schema.table_name 
# MAGIC UNSET TBLPROPERTIES ('comment');
# MAGIC
# MAGIC -- Or restore original comment
# MAGIC ALTER TABLE catalog.schema.table_name 
# MAGIC SET TBLPROPERTIES ('comment' = 'original_comment_here');
# MAGIC ```
# MAGIC
# MAGIC The original values (if any) are stored in the `original_value` column of the metadata tracking database.
# MAGIC
# MAGIC You can also query current table properties:
# MAGIC ```sql
# MAGIC SHOW TBLPROPERTIES catalog.schema.table_name;
# MAGIC ```
