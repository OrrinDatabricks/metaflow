"""
Test script to verify the Metadata Enrichment Tracker installation
"""
import sys
import importlib
from pathlib import Path

def test_imports():
    """Test if all required modules can be imported"""
    print("🧪 Testing imports...")
    
    required_modules = [
        'streamlit',
        'pandas',
        'psycopg2',
        'sqlalchemy',
        'plotly',
        'python-dotenv'
    ]
    
    failed_imports = []
    
    for module in required_modules:
        try:
            # Handle special cases
            if module == 'python-dotenv':
                importlib.import_module('dotenv')
            else:
                importlib.import_module(module)
            print(f"  ✅ {module}")
        except ImportError as e:
            print(f"  ❌ {module}: {e}")
            failed_imports.append(module)
    
    return len(failed_imports) == 0

def test_local_modules():
    """Test if local modules can be imported"""
    print("\n🏠 Testing local modules...")
    
    local_modules = [
        'config', 
        'database.database', 
        'app.utils'
    ]
    failed_imports = []
    
    for module in local_modules:
        try:
            importlib.import_module(module)
            print(f"  ✅ {module}")
        except ImportError as e:
            print(f"  ❌ {module}: {e}")
            failed_imports.append(module)
    
    return len(failed_imports) == 0

def test_file_structure():
    """Test if all required files exist"""
    print("\n📁 Testing file structure...")
    
    required_structure = {
        'app/app.py': 'Main Streamlit application',
        'app/utils.py': 'Utility functions',
        'database/database.py': 'Database models and operations',
        'database/generate_sample_data.py': 'Sample data generator',
        'code/bulk_column_description_generator.py': 'Column generator notebook',
        'code/bulk_table_description_generator.py': 'Table generator notebook',
        'code/bulk_column_description_importer.py': 'Column importer notebook',
        'code/bulk_table_description_importer.py': 'Table importer notebook',
        'config.py': 'Configuration management',
        'app/requirements.txt': 'Python dependencies',
        'app/databricks_app.yml': 'Databricks Apps configuration',
        'README.md': 'Documentation'
    }
    
    missing_files = []
    
    for file_path, description in required_structure.items():
        if Path(file_path).exists():
            print(f"  ✅ {file_path}")
        else:
            print(f"  ❌ {file_path} - {description}")
            missing_files.append(file_path)
    
    return len(missing_files) == 0

def test_database_connection():
    """Test database connection (if configured)"""
    print("\n🗄️  Testing database connection...")
    
    try:
        from database.database import get_db_manager
        db = get_db_manager()
        
        if db.engine is None:
            print("  ⚠️  Database not configured (this is OK for initial setup)")
            return True
        
        # Try to get stats (this will test the connection)
        stats = db.get_summary_stats()
        print(f"  ✅ Database connection successful")
        print(f"     Total items: {stats.get('total', 0)}")
        return True
        
    except Exception as e:
        print(f"  ❌ Database connection failed: {e}")
        print("     This is expected if you haven't configured your database yet")
        return False

def test_streamlit_app():
    """Test if Streamlit app can be loaded"""
    print("\n🎯 Testing Streamlit app...")
    
    try:
        # Try to import the main app module
        import app.app
        print("  ✅ App module loaded successfully")
        return True
    except Exception as e:
        print(f"  ❌ App loading failed: {e}")
        return False

def test_notebooks():
    """Test if notebook files exist and are properly structured"""
    print("\n📓 Testing notebook files...")
    
    notebook_files = [
        'code/bulk_column_description_generator.py',
        'code/bulk_table_description_generator.py', 
        'code/bulk_column_description_importer.py',
        'code/bulk_table_description_importer.py'
    ]
    
    all_exist = True
    for notebook in notebook_files:
        if Path(notebook).exists():
            print(f"  ✅ {notebook}")
        else:
            print(f"  ❌ {notebook}")
            all_exist = False
    
    return all_exist

def main():
    """Run all tests"""
    print("🧪 Metadata Enrichment Tracker - Installation Test")
    print("=" * 50)
    
    tests = [
        ("Required Python packages", test_imports),
        ("Local modules", test_local_modules),
        ("File structure", test_file_structure),
        ("Notebook files", test_notebooks),
        ("Database connection", test_database_connection),
        ("Streamlit app", test_streamlit_app)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"  ❌ Test failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Summary:")
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {test_name}")
        if result:
            passed += 1
    
    print(f"\n🎯 Results: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n🎉 All tests passed! Your installation looks good.")
        print("\nNext steps:")
        print("1. Run: streamlit run app/app.py")
        print("2. Upload notebooks to Databricks workspace")
        print("3. Configure database connection")
    else:
        print(f"\n⚠️  {len(results) - passed} test(s) failed.")
        print("Please check the errors above and:")
        print("1. Make sure you've run: pip install -r requirements.txt")
        print("2. Configure your .env file with database credentials")
        print("3. Check that all files are present in the correct folders")
        print("\nProject structure should be:")
        print("MetaFlow/")
        print("├── app/")
        print("│   ├── app.py")
        print("│   └── utils.py")
        print("├── database/")
        print("│   ├── database.py")
        print("│   └── generate_sample_data.py")
        print("├── code/")
        print("│   ├── bulk_column_description_generator.py")
        print("│   ├── bulk_table_description_generator.py")
        print("│   ├── bulk_column_description_importer.py")
        print("│   └── bulk_table_description_importer.py")
        print("├── config.py")
        print("├── requirements.txt")
        print("└── README.md")

if __name__ == "__main__":
    main()