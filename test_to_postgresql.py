import os
import sys
import pandas as pd
from dotenv import load_dotenv

# Add the project root to path
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# Load environment variables
load_dotenv()

# Import the function to test
from biz.pandas.to_postgresql import to_postgresql

def test_to_postgresql():
    """Test the to_postgresql function"""
    # Create a simple DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'david@example.com', 'eve@example.com'],
        'age': [25, 30, 35, 40, 45]
    })

    # Test parameters
    dbname = 'test_db'
    table_name = 'test_users'

    try:
        # Call the function to write to PostgreSQL
        to_postgresql(dbname=dbname, table_name=table_name, df=df)
        print("✓ Test completed successfully")
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        raise

if __name__ == "__main__":
    test_to_postgresql()