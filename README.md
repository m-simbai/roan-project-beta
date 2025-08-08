# PostgreSQL Database Connection

This project provides a simple Python interface to connect to a PostgreSQL database hosted on Render.

## Setup

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. The database connection string is stored in the `.env` file for security.

## Files

- `requirements.txt` - Python dependencies (psycopg2-binary, python-dotenv)
- `.env` - Environment variables (database connection string)
- `database_connection.py` - Main database connection class and test script

## Usage

### Basic Connection Test
```bash
python database_connection.py
```

### Using the DatabaseConnection Class
```python
from database_connection import DatabaseConnection

# Create connection
db = DatabaseConnection()

# Connect to database
if db.connect():
    # List all tables
    tables = db.get_tables()
    
    # Execute a query
    results = db.execute_query("SELECT * FROM your_table LIMIT 5;")
    
    # Close connection
    db.disconnect()
```

## Features

- ✅ Secure connection using environment variables
- ✅ Error handling and connection management
- ✅ Helper methods for common database operations
- ✅ Table listing and schema inspection
- ✅ Query execution with parameter binding

## Database Information

- **Host**: dpg-d28bhije5dus73fe8340-a.singapore-postgres.render.com
- **Database**: test_database_7jqj
- **Region**: Singapore
