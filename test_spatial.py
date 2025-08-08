import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

with engine.connect() as conn:
    # Check column info for natural_water table
    result = conn.execute(text("""
        SELECT column_name, data_type, udt_name 
        FROM information_schema.columns 
        WHERE table_name = 'natural_water'
        ORDER BY ordinal_position
    """))
    
    print("=== NATURAL_WATER TABLE COLUMNS ===")
    for row in result:
        print(f"Column: {row[0]}, Data Type: {row[1]}, UDT: {row[2]}")
        
    # Test spatial detection logic
    col_result = conn.execute(text("""
        SELECT column_name, data_type, udt_name 
        FROM information_schema.columns 
        WHERE table_name = 'natural_water'
    """))
    col_info = col_result.fetchall()
    
    has_spatial = any(
        row[2] == 'geometry' or row[2] == 'geography' or 
        row[0].lower() in ['geometry', 'geom', 'the_geom', 'wkb_geometry']
        for row in col_info
    )
    
    print(f"\nSpatial detection result: {has_spatial}")
    
    # Test sample geometry data
    try:
        sample = conn.execute(text("SELECT ST_AsText(geometry) as geom_text FROM natural_water LIMIT 1"))
        row = sample.fetchone()
        if row:
            print(f"Sample geometry: {row[0][:100]}...")
    except Exception as e:
        print(f"Error getting sample geometry: {e}")
