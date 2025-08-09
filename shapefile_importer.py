import os
import geopandas as gpd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ShapefileImporter:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        self.engine = None
    
    def create_engine_connection(self):
        """Create SQLAlchemy engine for GeoPandas"""
        try:
            db_url = self.database_url
            # Normalize for SQLAlchemy with pg8000 driver
            if db_url.startswith('postgres://'):
                db_url = 'postgresql://' + db_url[len('postgres://'):]
            if db_url.startswith('postgresql://') and '+pg8000' not in db_url:
                db_url = db_url.replace('postgresql://', 'postgresql+pg8000://', 1)

            self.engine = create_engine(db_url)
            print("[SUCCESS] SQLAlchemy engine created successfully!")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to create engine: {e}")
            return False
    
    def enable_postgis(self):
        """Enable PostGIS extension in the database"""
        try:
            with self.engine.connect() as conn:
                # Check if PostGIS is already enabled
                result = conn.execute(text("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'postgis');"))
                postgis_exists = result.scalar()
                
                if not postgis_exists:
                    # Try to create PostGIS extension
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
                    conn.commit()
                    print("[SUCCESS] PostGIS extension enabled!")
                else:
                    print("[INFO] PostGIS extension already enabled!")
                return True
        except Exception as e:
            print(f"[WARNING] Could not enable PostGIS extension: {e}")
            print("[INFO] The database might not have PostGIS available, but we'll try to import anyway.")
            return False
    
    def examine_shapefile(self, shapefile_path):
        """Examine the shapefile structure and contents"""
        try:
            print(f"[INFO] Examining shapefile: {shapefile_path}")
            
            # Read the shapefile
            gdf = gpd.read_file(shapefile_path)
            
            print(f"[INFO] Shapefile loaded successfully!")
            print(f"[INFO] Number of features: {len(gdf)}")
            print(f"[INFO] Coordinate Reference System: {gdf.crs}")
            print(f"[INFO] Geometry type: {gdf.geometry.geom_type.iloc[0] if len(gdf) > 0 else 'Unknown'}")
            print(f"[INFO] Columns: {list(gdf.columns)}")
            print(f"[INFO] Data types:")
            for col, dtype in gdf.dtypes.items():
                print(f"  - {col}: {dtype}")
            
            # Show first few rows (without geometry for readability)
            if len(gdf) > 0:
                print(f"\n[INFO] First 3 records (excluding geometry):")
                display_cols = [col for col in gdf.columns if col != 'geometry']
                if display_cols:
                    print(gdf[display_cols].head(3).to_string())
                else:
                    print("  Only geometry column found.")
            
            return gdf
        except Exception as e:
            print(f"[ERROR] Failed to examine shapefile: {e}")
            return None
    
    def import_shapefile(self, shapefile_path, table_name=None):
        """Import shapefile into PostgreSQL database"""
        try:
            # Read the shapefile
            gdf = gpd.read_file(shapefile_path)
            
            # Generate table name if not provided
            if table_name is None:
                table_name = os.path.splitext(os.path.basename(shapefile_path))[0].lower()
            
            print(f"[INFO] Importing shapefile to table: {table_name}")
            
            # Import to database
            gdf.to_postgis(
                name=table_name,
                con=self.engine,
                if_exists='replace',  # Replace if table already exists
                index=False
            )
            
            print(f"[SUCCESS] Shapefile imported successfully to table '{table_name}'!")
            
            # Verify the import
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
                count = result.scalar()
                print(f"[INFO] Verified: {count} records imported into '{table_name}' table.")
            
            return True
        except Exception as e:
            print(f"[ERROR] Failed to import shapefile: {e}")
            return False
    
    def list_imported_tables(self):
        """List all tables in the database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                """))
                tables = result.fetchall()
                
                if tables:
                    print(f"[INFO] Tables in database ({len(tables)}):")
                    for table in tables:
                        print(f"  - {table[0]}")
                else:
                    print("[INFO] No tables found in database.")
                return tables
        except Exception as e:
            print(f"[ERROR] Failed to list tables: {e}")
            return None

# Main execution
if __name__ == "__main__":
    shapefile_path = r"c:\Users\USER\Documents\Chewore\Database\Water\Natural.shp"
    
    importer = ShapefileImporter()
    
    if importer.create_engine_connection():
        # Enable PostGIS (optional, will continue even if it fails)
        importer.enable_postgis()
        
        # Examine the shapefile
        gdf = importer.examine_shapefile(shapefile_path)
        
        if gdf is not None:
            # Import the shapefile
            success = importer.import_shapefile(shapefile_path, "natural_water")
            
            if success:
                # List all tables to confirm
                importer.list_imported_tables()
        else:
            print("[ERROR] Could not examine shapefile. Import aborted.")
    else:
        print("[ERROR] Could not create database connection. Import aborted.")
