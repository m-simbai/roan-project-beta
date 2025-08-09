import os
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseConnection:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.database_url = os.getenv('DATABASE_URL')
    
    def connect(self):
        """Establish connection to PostgreSQL database"""
        try:
            self.connection = psycopg.connect(
                self.database_url,
                row_factory=dict_row
            )
            self.cursor = self.connection.cursor()
            print("[SUCCESS] Successfully connected to PostgreSQL database!")
            return True
        except psycopg.Error as e:
            print(f"[ERROR] Error connecting to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("[INFO] Database connection closed.")
    
    def execute_query(self, query, params=None):
        """Execute a SELECT query and return results"""
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except psycopg.Error as e:
            print(f"[ERROR] Error executing query: {e}")
            return None
    
    def execute_command(self, command, params=None):
        """Execute INSERT, UPDATE, DELETE commands"""
        try:
            self.cursor.execute(command, params)
            self.connection.commit()
            print("[SUCCESS] Command executed successfully!")
            return True
        except psycopg.Error as e:
            print(f"[ERROR] Error executing command: {e}")
            self.connection.rollback()
            return False
    
    def get_tables(self):
        """Get list of all tables in the database"""
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name;
        """
        return self.execute_query(query)
    
    def describe_table(self, table_name):
        """Get column information for a specific table"""
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        return self.execute_query(query, (table_name,))

# Test the connection
if __name__ == "__main__":
    db = DatabaseConnection()
    
    if db.connect():
        # List all tables
        tables = db.get_tables()
        if tables:
            print(f"\n[INFO] Tables in database ({len(tables)}):")
            for table in tables:
                print(f"  - {table['table_name']}")
        else:
            print("\n[INFO] No tables found in database.")
        
        # Get database version
        version = db.execute_query("SELECT version();")
        if version:
            print(f"\n[INFO] Database version: {version[0]['version']}")
        
        db.disconnect()
    else:
        print("Failed to connect to database.")
