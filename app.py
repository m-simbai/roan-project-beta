from flask import Flask, render_template, jsonify, send_file, Response, request, flash, redirect, url_for
import os
import tempfile
import zipfile
import io
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import json
import geopandas as gpd
from werkzeug.utils import secure_filename
import uuid

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev-key-change-in-production')

# Configure file uploads
UPLOAD_FOLDER = 'uploads'
MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size
ALLOWED_EXTENSIONS = {'zip'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Create upload directory if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is required")

engine = create_engine(DATABASE_URL)

@app.route('/')
def index():
    """Main page showing database overview"""
    try:
        with engine.connect() as conn:
            # Get all tables
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            # Filter out PostGIS system tables
            user_tables = [t for t in tables if not t.startswith(('spatial_ref_sys', 'geography_columns', 'geometry_columns'))]
            
            # Get table info
            table_info = []
            for table in user_tables:
                try:
                    # Get row count
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    
                    # Get columns using information_schema (more reliable for PostGIS)
                    col_result = conn.execute(text(f"""
                        SELECT column_name, data_type, udt_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{table}'
                        ORDER BY ordinal_position
                    """))
                    col_info = col_result.fetchall()
                    col_names = [row[0] for row in col_info]
                    
                    # Check for spatial columns (geometry, geography, or common spatial column names)
                    has_spatial = any(
                        row[2] == 'geometry' or row[2] == 'geography' or 
                        row[0].lower() in ['geometry', 'geom', 'the_geom', 'wkb_geometry']
                        for row in col_info
                    )
                    
                    table_info.append({
                        'name': table,
                        'count': count,
                        'columns': col_names,
                        'has_spatial': has_spatial
                    })
                except Exception as e:
                    print(f"Error getting info for table {table}: {e}")
            
            return render_template('index.html', tables=table_info)
    except Exception as e:
        return f"Database connection error: {e}"

@app.route('/table/<table_name>')
def view_table(table_name):
    """View contents of a specific table"""
    try:
        with engine.connect() as conn:
            # Get table structure
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            
            # Get sample data (first 50 rows)
            # Handle geometry columns specially
            col_names = [col['name'] for col in columns]
            
            # For geometry columns, convert to text representation with proper column quoting
            select_cols = []
            for col in col_names:
                if any(c['name'] == col and str(c['type']).lower().startswith('geometry') for c in columns):
                    select_cols.append(f"ST_AsText(\"{col}\") as \"{col}\"")
                else:
                    select_cols.append(f"\"{col}\"")
            
            query = f"SELECT {', '.join(select_cols)} FROM \"{table_name}\" LIMIT 50"
            result = conn.execute(text(query))
            rows = result.fetchall()
            
            # Convert to list of dictionaries
            data = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(col_names):
                    row_dict[col] = row[i]
                data.append(row_dict)
            
            return render_template('table.html', 
                                 table_name=table_name, 
                                 columns=col_names, 
                                 data=data,
                                 total_rows=len(data))
    except Exception as e:
        return f"Error viewing table {table_name}: {e}"

@app.route('/api/table/<table_name>')
def api_table_data(table_name):
    """API endpoint to get table data as JSON"""
    try:
        with engine.connect() as conn:
            # Get table structure
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            col_names = [col['name'] for col in columns]
            
            # Get data
            select_cols = []
            for col in col_names:
                if any(c['name'] == col and str(c['type']).lower().startswith('geometry') for c in columns):
                    select_cols.append(f"ST_AsGeoJSON({col}) as {col}")
                else:
                    select_cols.append(col)
            
            query = f"SELECT {', '.join(select_cols)} FROM {table_name} LIMIT 100"
            result = conn.execute(text(query))
            rows = result.fetchall()
            
            # Convert to list of dictionaries
            data = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(col_names):
                    value = row[i]
                    # Try to parse GeoJSON for geometry columns
                    if col == 'geometry' and isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except:
                            pass
                    row_dict[col] = value
                data.append(row_dict)
            
            return jsonify({
                'table': table_name,
                'columns': col_names,
                'data': data,
                'count': len(data)
            })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/geojson/<table_name>')
def api_geojson(table_name):
    """API endpoint to get spatial data as GeoJSON FeatureCollection"""
    try:
        with engine.connect() as conn:
            # Get table structure using PostGIS metadata
            col_result = conn.execute(text(f"""
                SELECT column_name, data_type, udt_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """))
            col_info = col_result.fetchall()
            
            # Find geometry column
            geometry_col = None
            for row in col_info:
                if (row[2] == 'geometry' or row[2] == 'geography' or 
                    row[0].lower() in ['geometry', 'geom', 'the_geom', 'wkb_geometry']):
                    geometry_col = row[0]
                    break
            
            if not geometry_col:
                return jsonify({'error': 'No geometry column found in table'})
            
            # Get all non-geometry columns for properties
            property_cols = [row[0] for row in col_info if row[0] != geometry_col]
            
            if not property_cols:
                # If no other columns, just return geometry
                query = text(f"""
                    SELECT jsonb_build_object(
                        'type', 'Feature',
                        'id', row_number() OVER (),
                        'geometry', ST_AsGeoJSON({geometry_col})::jsonb,
                        'properties', jsonb_build_object()
                    ) as feature
                    FROM {table_name}
                    WHERE {geometry_col} IS NOT NULL
                    LIMIT 1000
                """)
            else:
                # Build properties object dynamically with proper column name quoting
                properties_parts = []
                for col in property_cols:
                    # Quote column names to handle case sensitivity and special characters
                    properties_parts.append(f"'{col}', \"{col}\"")
                properties_sql = ', '.join(properties_parts)
                
                query = text(f"""
                    SELECT jsonb_build_object(
                        'type', 'Feature',
                        'id', row_number() OVER (),
                        'geometry', ST_AsGeoJSON(\"{geometry_col}\")::jsonb,
                        'properties', jsonb_build_object({properties_sql})
                    ) as feature
                    FROM \"{table_name}\"
                    WHERE \"{geometry_col}\" IS NOT NULL
                    LIMIT 1000
                """)
            
            result = conn.execute(query)
            features = [row[0] for row in result.fetchall()]
            
            # Build FeatureCollection
            geojson = {
                'type': 'FeatureCollection',
                'features': features
            }
            
            return jsonify(geojson)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/geojson/<table_name>/filtered')
def api_geojson_filtered(table_name):
    """API endpoint to get filtered spatial data as GeoJSON FeatureCollection"""
    filter_query = request.args.get('q', '').strip()
    
    if not filter_query:
        # If no filter, redirect to regular geojson endpoint
        return api_geojson(table_name)
    
    try:
        with engine.connect() as conn:
            # Get table structure using PostGIS metadata
            col_result = conn.execute(text(f"""
                SELECT column_name, data_type, udt_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """))
            col_info = col_result.fetchall()
            
            # Find geometry column
            geometry_col = None
            for row in col_info:
                if (row[2] == 'geometry' or row[2] == 'geography' or 
                    row[0].lower() in ['geometry', 'geom', 'the_geom', 'wkb_geometry']):
                    geometry_col = row[0]
                    break
            
            if not geometry_col:
                return jsonify({'error': 'No geometry column found in table'})
            
            # Get text columns for filtering
            search_cols = []
            for row in col_info:
                col_name = row[0]
                data_type = row[1].lower()
                if any(t in data_type for t in ['text', 'varchar', 'char', 'string']) and col_name != geometry_col:
                    search_cols.append(col_name)
            
            # Build filter conditions
            where_conditions = []
            search_params = {}
            for i, col in enumerate(search_cols):
                param_name = f'param_{i}'
                where_conditions.append(f'LOWER(CAST("{col}" AS TEXT)) LIKE LOWER(:{param_name})')
                search_params[param_name] = f'%{filter_query}%'
            
            # Build filtered query
            other_cols = [row[0] for row in col_info if row[0] != geometry_col and row[2] != 'geometry']
            col_list = ', '.join([f'"{col}"' for col in other_cols])
            
            if where_conditions:
                query = f"""
                    SELECT {col_list}, ST_AsGeoJSON("{geometry_col}") as geojson_geom
                    FROM "{table_name}"
                    WHERE {' OR '.join(where_conditions)}
                    LIMIT 1000
                """
                result = conn.execute(text(query), search_params)
            else:
                # If no text columns to search, return empty result
                return jsonify({
                    'type': 'FeatureCollection',
                    'features': [],
                    'filter_applied': True,
                    'filter_query': filter_query
                })
            
            # Build GeoJSON FeatureCollection
            features = []
            for row in result:
                row_dict = dict(row._mapping)
                geom_json = row_dict.pop('geojson_geom')
                
                if geom_json:
                    try:
                        geometry = json.loads(geom_json)
                        feature = {
                            'type': 'Feature',
                            'geometry': geometry,
                            'properties': row_dict
                        }
                        features.append(feature)
                    except json.JSONDecodeError:
                        continue
            
            geojson = {
                'type': 'FeatureCollection',
                'features': features,
                'filter_applied': True,
                'filter_query': filter_query,
                'total_features': len(features)
            }
            
            return jsonify(geojson)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/map/<table_name>')
def map_view(table_name):
    """Map view for spatial tables"""
    # Get filter parameters from URL
    filter_query = request.args.get('filter', '')
    filter_type = request.args.get('filter_type', 'all')  # 'all', 'search', or 'custom'
    
    try:
        with engine.connect() as conn:
            # Check if table has geometry column using PostGIS metadata
            col_result = conn.execute(text(f"""
                SELECT column_name, data_type, udt_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """))
            col_info = col_result.fetchall()
            
            # Check for spatial columns
            has_geometry = any(
                row[2] == 'geometry' or row[2] == 'geography' or 
                row[0].lower() in ['geometry', 'geom', 'the_geom', 'wkb_geometry']
                for row in col_info
            )
            
            if not has_geometry:
                return f"Table '{table_name}' does not contain spatial data."
            
            # Get table bounds for initial map view
            try:
                bounds_query = f"""
                SELECT 
                    ST_XMin(extent) as min_x,
                    ST_YMin(extent) as min_y, 
                    ST_XMax(extent) as max_x,
                    ST_YMax(extent) as max_y
                FROM (
                    SELECT ST_Extent(geometry) as extent 
                    FROM {table_name} 
                    WHERE geometry IS NOT NULL
                ) as subquery
                """
                result = conn.execute(text(bounds_query))
                bounds = result.fetchone()
                
                if bounds and all(b is not None for b in bounds):
                    map_bounds = {
                        'min_x': float(bounds[0]),
                        'min_y': float(bounds[1]),
                        'max_x': float(bounds[2]),
                        'max_y': float(bounds[3])
                    }
                else:
                    map_bounds = None
            except:
                map_bounds = None
            
            # Get record count
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            total_count = count_result.scalar()
            
            return render_template('map.html', 
                                 table_name=table_name,
                                 map_bounds=map_bounds,
                                 total_count=total_count,
                                 filter_query=filter_query,
                                 filter_type=filter_type)
    except Exception as e:
        return f"Error loading map for table {table_name}: {e}"

@app.route('/upload', methods=['GET', 'POST'])
def upload_shapefile():
    """Upload shapefile page and handler"""
    if request.method == 'GET':
        return render_template('upload.html')
    
    # Handle POST request (file upload)
    if 'file' not in request.files:
        return jsonify({'error': 'No file selected'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Only ZIP files are allowed'}), 400
    
    try:
        # Generate unique filename
        upload_id = str(uuid.uuid4())
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{upload_id}_{filename}")
        
        # Save uploaded file
        file.save(filepath)
        
        # Process the shapefile in background
        result = process_shapefile_upload(filepath, upload_id)
        
        if result['success']:
            # Clean up uploaded file
            os.remove(filepath)
            return jsonify({
                'success': True,
                'message': f"Shapefile imported successfully as table '{result['table_name']}'!",
                'table_name': result['table_name'],
                'record_count': result['record_count']
            })
        else:
            # Clean up uploaded file on error
            if os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({'error': result['error']}), 500
            
    except Exception as e:
        # Clean up uploaded file on error
        if 'filepath' in locals() and os.path.exists(filepath):
            os.remove(filepath)
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

def process_shapefile_upload(zip_filepath, upload_id):
    """Process uploaded shapefile ZIP and import to database"""
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract ZIP file
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Find .shp file
            shp_files = [f for f in os.listdir(temp_dir) if f.endswith('.shp')]
            if not shp_files:
                return {'success': False, 'error': 'No shapefile (.shp) found in ZIP'}
            
            if len(shp_files) > 1:
                return {'success': False, 'error': 'Multiple shapefiles found in ZIP. Please upload one shapefile at a time.'}
            
            shp_file = shp_files[0]
            shp_path = os.path.join(temp_dir, shp_file)
            
            # Generate table name from filename
            base_name = os.path.splitext(shp_file)[0].lower()
            # Clean table name (remove special characters, ensure it starts with letter)
            table_name = ''.join(c if c.isalnum() else '_' for c in base_name)
            if not table_name[0].isalpha():
                table_name = 'table_' + table_name
            
            # Check if table already exists
            with engine.connect() as conn:
                existing_tables = conn.execute(text(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                )).fetchall()
                existing_names = [row[0] for row in existing_tables]
                
                # Make table name unique if necessary
                original_name = table_name
                counter = 1
                while table_name in existing_names:
                    table_name = f"{original_name}_{counter}"
                    counter += 1
            
            # Read and import shapefile
            gdf = gpd.read_file(shp_path)
            
            if len(gdf) == 0:
                return {'success': False, 'error': 'Shapefile contains no features'}
            
            # Import to PostGIS
            gdf.to_postgis(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False
            )
            
            return {
                'success': True,
                'table_name': table_name,
                'record_count': len(gdf)
            }
            
    except Exception as e:
        return {'success': False, 'error': f'Processing failed: {str(e)}'}

@app.route('/download/shapefile/<table_name>')
def download_shapefile(table_name):
    """Download spatial table as shapefile ZIP"""
    try:
        with engine.connect() as conn:
            # Check if table has spatial data
            col_result = conn.execute(text(f"""
                SELECT column_name, data_type, udt_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """))
            col_info = col_result.fetchall()
            
            # Check for spatial columns
            has_geometry = any(
                row[2] == 'geometry' or row[2] == 'geography' or 
                row[0].lower() in ['geometry', 'geom', 'the_geom', 'wkb_geometry']
                for row in col_info
            )
            
            if not has_geometry:
                return jsonify({'error': 'Table does not contain spatial data'}), 400
            
            # Read spatial data using GeoPandas
            query = f"SELECT * FROM {table_name}"
            gdf = gpd.read_postgis(query, conn, geom_col='geometry')
            
            if len(gdf) == 0:
                return jsonify({'error': 'No data found in table'}), 404
            
            # Create temporary directory for shapefile components
            with tempfile.TemporaryDirectory() as temp_dir:
                shapefile_path = os.path.join(temp_dir, f"{table_name}.shp")
                
                # Write shapefile
                gdf.to_file(shapefile_path, driver='ESRI Shapefile')
                
                # Create ZIP file in memory
                zip_buffer = io.BytesIO()
                
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    # Add all shapefile components to ZIP
                    shapefile_extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg']
                    base_name = os.path.join(temp_dir, table_name)
                    
                    for ext in shapefile_extensions:
                        file_path = base_name + ext
                        if os.path.exists(file_path):
                            zip_file.write(file_path, f"{table_name}{ext}")
                
                zip_buffer.seek(0)
                
                # Return ZIP file as download
                return Response(
                    zip_buffer.getvalue(),
                    mimetype='application/zip',
                    headers={
                        'Content-Disposition': f'attachment; filename="{table_name}_shapefile.zip"',
                        'Content-Type': 'application/zip'
                    }
                )
                
    except Exception as e:
        return jsonify({'error': f'Failed to create shapefile: {str(e)}'}), 500

@app.route('/api/search')
def search_database():
    """Enhanced search API that searches inside attribute tables and column values"""
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({'results': []})
    
    try:
        results = []
        
        with engine.connect() as conn:
            # Get all tables
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            
            for table_name in table_names:
                try:
                    # Get table columns
                    columns = inspector.get_columns(table_name)
                    col_names = [col['name'] for col in columns]
                    
                    # Check if table name matches
                    table_match = query.lower() in table_name.lower()
                    
                    # Check if any column name matches
                    column_matches = [col for col in col_names if query.lower() in col.lower()]
                    
                    # Search in table data (limit to prevent performance issues)
                    data_matches = []
                    search_cols = []
                    
                    # Only search in text/varchar columns to avoid type errors
                    for col in columns:
                        col_type = str(col['type']).lower()
                        if any(t in col_type for t in ['text', 'varchar', 'char', 'string']):
                            search_cols.append(col['name'])
                    
                    if search_cols:
                        # Build search query for data content
                        where_conditions = []
                        search_params = {}
                        for i, col in enumerate(search_cols):
                            param_name = f'param_{i}'
                            where_conditions.append(f'LOWER(CAST("{col}" AS TEXT)) LIKE LOWER(:{param_name})')
                            search_params[param_name] = f'%{query}%'
                        
                        if where_conditions:
                            search_query = f'''
                                SELECT COUNT(*) as match_count
                                FROM "{table_name}"
                                WHERE {' OR '.join(where_conditions)}
                            '''
                            
                            result = conn.execute(text(search_query), search_params)
                            match_count = result.scalar() or 0
                            
                            if match_count > 0:
                                # Get sample matching records
                                sample_query = f'''
                                    SELECT * FROM "{table_name}"
                                    WHERE {' OR '.join(where_conditions)}
                                    LIMIT 3
                                '''
                                sample_result = conn.execute(text(sample_query), search_params)
                                sample_records = sample_result.fetchall()
                                
                                for record in sample_records:
                                    record_dict = dict(record._mapping)
                                    # Find which columns contain the search term
                                    matching_fields = {}
                                    for col in search_cols:
                                        if col in record_dict and record_dict[col]:
                                            value = str(record_dict[col])
                                            if query.lower() in value.lower():
                                                matching_fields[col] = value
                                    
                                    if matching_fields:
                                        data_matches.append({
                                            'record_id': record_dict.get('id', 'N/A'),
                                            'matching_fields': matching_fields
                                        })
                    
                    # Check if table has spatial data
                    has_spatial = any(
                        col['name'].lower() in ['geometry', 'geom', 'the_geom', 'wkb_geometry'] or 
                        'geometry' in str(col['type']).lower()
                        for col in columns
                    )
                    
                    # Add to results if any matches found
                    if table_match or column_matches or data_matches:
                        results.append({
                            'table_name': table_name,
                            'table_match': table_match,
                            'column_matches': column_matches,
                            'data_matches': data_matches[:3],  # Limit to 3 sample matches
                            'total_data_matches': len(data_matches),
                            'has_spatial': has_spatial,
                            'total_columns': len(col_names),
                            'relevance_score': (
                                (10 if table_match else 0) +
                                (len(column_matches) * 5) +
                                (min(len(data_matches), 10) * 2)
                            )
                        })
                        
                except Exception as table_error:
                    # Skip tables that cause errors (e.g., permission issues)
                    continue
            
            # Sort results by relevance score
            results.sort(key=lambda x: x['relevance_score'], reverse=True)
            
            return jsonify({
                'query': query,
                'total_results': len(results),
                'results': results[:20]  # Limit to top 20 results
            })
            
    except Exception as e:
        return jsonify({'error': f'Search failed: {str(e)}'}), 500

if __name__ == '__main__':
    # Run Flask app with debug mode but disable auto-reload to prevent upload interruptions
    app.run(
        debug=True, 
        host='0.0.0.0', 
        port=5000,
        use_reloader=False  # Disable auto-reload to prevent upload interruptions
    )
