"""
Setup script for Metadata Enrichment Tracker
"""
import os
import sys
import subprocess
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        return False
    print(f"✅ Python {sys.version} is compatible")
    return True

def install_requirements():
    """Install required packages"""
    print("📦 Installing requirements...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "app/requirements.txt"])
        print("✅ Requirements installed successfully")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to install requirements")
        return False

def create_env_file():
    """Create environment file from template"""
    env_file = Path(".env")
    if env_file.exists():
        print("⚠️  .env file already exists, skipping creation")
        return True
    
    print("📝 Creating .env file...")
    env_content = """# Databricks connection settings
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=your-access-token

# Database settings (for Lakebase/Postgres)
DB_HOST=your-postgres-host
DB_PORT=5432
DB_NAME=your-database-name
DB_USER=your-username
DB_PASSWORD=your-password
DB_SCHEMA=metadata_enrichment
"""
    
    try:
        with open(".env", "w") as f:
            f.write(env_content)
        print("✅ .env file created - please update with your actual values")
        return True
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")
        return False

def setup_database():
    """Setup database tables"""
    print("🗄️  Setting up database...")
    try:
        from database.database import get_db_manager
        db = get_db_manager()
        if db.create_tables():
            print("✅ Database tables created successfully")
            return True
        else:
            print("❌ Failed to create database tables")
            return False
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False

def generate_sample_data_prompt():
    """Ask user if they want to generate sample data"""
    response = input("🎯 Would you like to generate sample data for testing? (y/n): ").lower().strip()
    if response == 'y':
        try:
            from database.generate_sample_data import insert_sample_data
            if insert_sample_data():
                print("✅ Sample data generated successfully")
                return True
            else:
                print("❌ Failed to generate sample data")
                return False
        except Exception as e:
            print(f"❌ Sample data generation failed: {e}")
            return False
    else:
        print("⏭️  Skipping sample data generation")
        return True

def main():
    """Main setup function"""
    print("🚀 Metadata Enrichment Tracker Setup")
    print("=" * 40)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Install requirements
    if not install_requirements():
        sys.exit(1)
    
    # Create .env file
    if not create_env_file():
        sys.exit(1)
    
    print("\n⚠️  IMPORTANT: Please update the .env file with your actual database credentials before proceeding.")
    input("Press Enter when you have updated the .env file...")
    
    # Setup database
    if not setup_database():
        print("⚠️  Database setup failed. Please check your configuration and try again.")
        sys.exit(1)
    
    # Generate sample data
    if not generate_sample_data_prompt():
        print("⚠️  Sample data generation failed, but you can continue without it.")
    
    print("\n🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Verify your .env file has correct database credentials")
    print("2. Run the application: streamlit run app/app.py")
    print("3. Open your browser to the displayed URL")
    print("\nFor Databricks Apps deployment:")
    print("1. Upload all files to your Databricks workspace")
    print("2. Configure environment variables in Apps settings")
    print("3. Deploy using the app/databricks_app.yml configuration")
    print("\nProject Structure:")
    print("├── app/           # Streamlit application files")
    print("├── database/      # Database models and utilities")
    print("├── code/          # Databricks notebooks")
    print("├── config.py      # Configuration management")
    print("└── requirements.txt # Python dependencies")

if __name__ == "__main__":
    main()