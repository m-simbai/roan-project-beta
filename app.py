from flask import Flask, render_template, jsonify, send_file, Response, request, flash, redirect, url_for
from werkzeug.exceptions import RequestEntityTooLarge
import os
import tempfile
import zipfile
import datetime
import io
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

import json
import geopandas as gpd
try:
    from geoalchemy2 import Geometry
except Exception:
    Geometry = None
from werkzeug.utils import secure_filename
import uuid

# Load environment variables
load_dotenv()

def make_db_engine(isolation_level='AUTOCOMMIT'):
    """Create a new database engine with proper settings."""
    url = os.getenv('DATABASE_URL')
    if not url:
        raise ValueError("DATABASE_URL environment variable is required")
    
    # Normalize database URL
    if url.startswith('postgres://'):
        url = 'postgresql://' + url[len('postgres://'):]
    
    # Force psycopg2 driver
    if url.startswith('postgresql+pg8000://'):
        url = url.replace('postgresql+pg8000://', 'postgresql+psycopg2://', 1)
    elif url.startswith('postgresql://') and '+psycopg2' not in url:
        url = url.replace('postgresql://', 'postgresql+psycopg2://', 1)
    
    # Ensure SSL and short connect timeout for Render / cloud Postgres
    if url.startswith('postgresql+psycopg2://'):
        if 'sslmode=' not in url:
            sep = '&' if '?' in url else '?'
            url = f"{url}{sep}sslmode=require"
        # Fast fail if DB is unreachable
        connect_args = {
            'connect_timeout': 3,  # seconds
            'sslmode': 'require',
            # Fail fast on queries too
            'options': '-c statement_timeout=2000'
        }
    else:
        connect_args = {}

    return create_engine(
        url,
        isolation_level=isolation_level,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_timeout=5,
        connect_args=connect_args
    )

# Create the main database engine, using AUTOCOMMIT for safety on read-only pages
engine = make_db_engine()

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

def _derive_table_name(preferred: str, original_filename: str) -> str:
    base = (preferred or os.path.splitext(original_filename)[0]).strip()
    # Basic normalization
    safe = ''.join(c if (c.isalnum() or c == '_') else '_' for c in base)
    if safe and safe[0].isdigit():
        safe = f"t_{safe}"
    return safe.lower() or f"t_{uuid.uuid4().hex[:8]}"

def _ingest_zip_to_postgis(zip_path: str, table_name: str):
    # Extract ZIP to temp dir and find first .shp
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(tmpdir)
        shp_path = None
        for root, _, files in os.walk(tmpdir):
            for fn in files:
                if fn.lower().endswith('.shp'):
                    shp_path = os.path.join(root, fn)
                    break
            if shp_path:
                break
        if not shp_path:
            raise ValueError('No .shp found inside the ZIP')
        # Read with GeoPandas
        gdf = gpd.read_file(shp_path)
        if gdf.empty:
            raise ValueError('Shapefile contains no features')
        # Ensure CRS
        if gdf.crs is None:
            # Assume WGS84 if missing; adjust if your data differs
            gdf.set_crs(epsg=4326, inplace=True)
        else:
            try:
                gdf = gdf.to_crs(epsg=4326)
            except Exception:
                pass
        # Standardize geometry column name to 'geom'
        if gdf.geometry.name != 'geom':
            gdf = gdf.rename_geometry('geom')
        # Write to PostGIS
        dtype = None
        if Geometry is not None:
            dtype = {'geom': Geometry('GEOMETRY', srid=4326)}
        # Replace existing table if it exists
        gdf.to_postgis(table_name, engine, if_exists='replace', index=False, dtype=dtype)
        return {
            'table': table_name,
            'feature_count': int(len(gdf)),
            'crs': str(gdf.crs) if gdf.crs else None
        }

# Upload API: accepts multipart/form-data with fields: name (optional), file (required .zip)
@app.route('/api/upload', methods=['POST'])
def api_upload():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part in the request'}), 400
        file = request.files['file']
        if not file or file.filename == '':
            return jsonify({'error': 'No selected file'}), 400
        if not allowed_file(file.filename):
            return jsonify({'error': 'Unsupported file type. Please upload a .zip file.'}), 400
        original_name = secure_filename(file.filename)
        uid = uuid.uuid4().hex
        saved_name = f"{uid}_{original_name}"
        save_path = os.path.join(app.config['UPLOAD_FOLDER'], saved_name)
        file.save(save_path)
        size = os.path.getsize(save_path)

        preferred_name = request.form.get('name', '').strip()
        table_name = _derive_table_name(preferred_name, original_name)

        # Ingest into PostGIS
        ingest_info = _ingest_zip_to_postgis(save_path, table_name)

        return jsonify({
            'ok': True,
            'filename': original_name,
            'saved_as': saved_name,
            'size_bytes': size,
            'message': 'File uploaded and ingested successfully',
            **ingest_info
        })
    except RequestEntityTooLarge:
        return jsonify({'error': 'File too large. Maximum allowed is 100MB.'}), 413
    except Exception as e:
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

# Helper you can call manually if needed to inspect routing
def log_url_map_once():
    try:
        print("\n=== Flask URL Map ===")
        for rule in sorted(app.url_map.iter_rules(), key=lambda r: r.rule):
            print(f"  {rule}")
        print("=== End URL Map ===\n")
    except Exception as e:
        print("Failed to log URL map:", e)

# Expose Google Maps API key to all templates
@app.context_processor
def inject_google_maps_api_key():
    return {
        'GOOGLE_MAPS_API_KEY': os.getenv('GOOGLE_MAPS_API_KEY', '')
    }

# Ensure JSON is returned for common errors on AJAX upload
@app.errorhandler(RequestEntityTooLarge)
def handle_file_too_large(e):
    # Triggered when MAX_CONTENT_LENGTH is exceeded
    return jsonify({'error': 'File too large. Maximum allowed is 100MB.'}), 413

@app.errorhandler(500)
def handle_internal_error(e):
    # If the request was the upload AJAX, respond with JSON to avoid HTML pages
    if request.path == '/upload' or 'application/json' in (request.headers.get('Accept') or ''):
        return jsonify({'error': 'An unexpected server error occurred during upload.'}), 500
    # Fallback to default behavior for non-AJAX routes
    return render_template('error.html', message=str(e)), 500

# Create the main database engine (defined once; function with isolation_level parameter is at top)
engine = make_db_engine()

@app.route('/')
def index():
    """Redirect root to the new Glass UI."""
    return redirect(url_for('glass_ui'), code=302)

@app.route('/glass')
def glass_ui():
    """Serve the new Glass UI template for testing/usage."""
    return render_template('Roan Project Spatial Database UI.html')

# Basic favicon handler to avoid noisy 404s in console
@app.route('/favicon.ico')
def favicon():
    return ('', 204)

# Redirect any unknown routes to the Glass UI so the new interface is default everywhere
@app.errorhandler(404)
def handle_404(e):
    return redirect(url_for('glass_ui'))

# Optional catch-all route to aggressively route everything to /glass
@app.route('/<path:unused_path>')
def catch_all(unused_path):
    return redirect(url_for('glass_ui'))

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
            
            # Legacy template removed; redirect to main Glass UI
            return redirect(url_for('glass_ui'))
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

@app.route('/api/status')
def api_status():
    """Simple health/status endpoint to report database connectivity."""
    # Log the status check attempt
    print("\n=== Database Status Check ===")
    print(f"Time: {datetime.datetime.now()}")
    print(f"Database URL: {os.environ.get('DATABASE_URL', 'Not set')[:20]}...")
    
    temp_engine = None
    try:
        # Create a fresh engine for this check
        print("Creating new database engine...")
        temp_engine = make_db_engine()
        
        # Try a lightweight query with a short timeout
        print("Attempting to connect to database...")
        with temp_engine.connect() as conn:
            print("Connection established, running test query...")
            result = conn.execute(text("SELECT 1"))
            print("Query executed successfully")
            
            # Verify we got a result
            if result.scalar() == 1:
                status = {
                    'status': 'connected',
                    'message': 'Database connection successful',
                    'timestamp': datetime.datetime.now().isoformat()
                }
                print("Status: CONNECTED")
                return jsonify(status)
            else:
                raise Exception("Unexpected query result")
                
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"ERROR: {error_type} - {error_msg}")
        
        # Return detailed error response
        return jsonify({
            'status': 'disconnected',
            'message': f'Database connection failed: {error_msg}',
            'error_type': error_type,
            'timestamp': datetime.datetime.now().isoformat()
        }), 200
        
    finally:
        # Ensure the temporary engine is disposed of properly
        if temp_engine:
            print("Cleaning up database engine...\n")
            temp_engine.dispose()

@app.route('/api/stats')
def api_stats():
    """Return sidebar table statistics: polygons area/count, polylines length/count, points count.
    Uses PostGIS functions and aggregates over all public tables having a geometry column.
    Distances/areas are in meters via geography casts.
    """
    try:
        with engine.connect() as conn:
            # Ensure PostGIS exists quickly; if missing, return zeros with hint
            try:
                conn.execute(text("SELECT PostGIS_Full_Version()"))
            except Exception as e:
                return jsonify({
                    'polygons': {'feature_count': 0, 'total_area_m2': 0.0, 'tables': []},
                    'lines': {'feature_count': 0, 'total_length_m': 0.0, 'tables': []},
                    'points': {'feature_count': 0, 'tables': []},
                    'note': 'PostGIS is not enabled on this database.'
                })

            # Discover tables with geometry columns in public schema
            geom_rows = conn.execute(text(
                """
                SELECT table_name, column_name
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND udt_name = 'geometry'
                """
            )).fetchall()

            polygons = {'feature_count': 0, 'total_area_m2': 0.0, 'tables': []}
            lines = {'feature_count': 0, 'total_length_m': 0.0, 'tables': []}
            points = {'feature_count': 0, 'tables': []}

            for table_name, col in geom_rows:
                # Determine dominant geometry type for this column
                try:
                    gtype_row = conn.execute(text(
                        f"SELECT UPPER(Replace(ST_GeometryType(\"{col}\"), 'ST_', '')) as gtype \n"
                        f"FROM \"{table_name}\" WHERE \"{col}\" IS NOT NULL LIMIT 1"
                    )).fetchone()
                    if not gtype_row or not gtype_row[0]:
                        continue
                    gtype = gtype_row[0]
                except Exception:
                    continue

                # Compute per-table stats depending on type family
                if 'POLYGON' in gtype:
                    q = text(
                        f"SELECT COUNT(*) as cnt, COALESCE(SUM(ST_Area(\"{col}\"::geography)),0) as area FROM \"{table_name}\" WHERE \"{col}\" IS NOT NULL"
                    )
                    row = conn.execute(q).fetchone()
                    cnt = int(row[0] or 0)
                    area = float(row[1] or 0.0)
                    if cnt:
                        polygons['feature_count'] += cnt
                        polygons['total_area_m2'] += area
                        polygons['tables'].append({'table': table_name, 'count': cnt, 'area_m2': area})
                elif 'LINESTRING' in gtype:
                    q = text(
                        f"SELECT COUNT(*) as cnt, COALESCE(SUM(ST_Length(\"{col}\"::geography)),0) as len FROM \"{table_name}\" WHERE \"{col}\" IS NOT NULL"
                    )
                    row = conn.execute(q).fetchone()
                    cnt = int(row[0] or 0)
                    length_m = float(row[1] or 0.0)
                    if cnt:
                        lines['feature_count'] += cnt
                        lines['total_length_m'] += length_m
                        lines['tables'].append({'table': table_name, 'count': cnt, 'length_m': length_m})
                elif 'POINT' in gtype:
                    q = text(
                        f"SELECT COUNT(*) as cnt FROM \"{table_name}\" WHERE \"{col}\" IS NOT NULL"
                    )
                    row = conn.execute(q).fetchone()
                    cnt = int(row[0] or 0)
                    if cnt:
                        points['feature_count'] += cnt
                        points['tables'].append({'table': table_name, 'count': cnt})
                else:
                    # Skip unsupported geometry types for the sidebar summary
                    continue

            # Round totals to sensible precision
            polygons['total_area_m2'] = round(polygons['total_area_m2'], 2)
            lines['total_length_m'] = round(lines['total_length_m'], 2)

            return jsonify({'polygons': polygons, 'lines': lines, 'points': points})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tables')
def api_tables():
    """List tables with basic metadata used by the Glass UI."""
    try:
        with engine.connect() as conn:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            user_tables = [t for t in tables if not t.startswith(('spatial_ref_sys', 'geography_columns', 'geometry_columns'))]

            table_info = []
            for table in user_tables:
                try:
                    # Count rows
                    result = conn.execute(text(f"SELECT COUNT(*) FROM \"{table}\""))
                    count = result.scalar() or 0

                    # Column info
                    col_result = conn.execute(text(f"""
                        SELECT column_name, data_type, udt_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{table}'
                        ORDER BY ordinal_position
                    """))
                    col_info = col_result.fetchall()
                    col_names = [row[0] for row in col_info]

                    # Spatial flag
                    has_spatial = any(
                        row[2] == 'geometry' or row[2] == 'geography' or 
                        row[0].lower() in ['geometry', 'geom', 'the_geom', 'wkb_geometry']
                        for row in col_info
                    )

                    table_info.append({
                        'name': table,
                        'count': int(count),
                        'columns': col_names,
                        'has_spatial': has_spatial
                    })
                except Exception as inner_e:
                    # Skip problematic table but continue
                    print(f"/api/tables: error on table {table}: {inner_e}")

            return jsonify({'tables': table_info})
    except Exception as e:
        return jsonify({'error': str(e), 'tables': []})

# Diagnostics: list all registered routes to verify active endpoints in the running server
@app.route('/api/routes')
def api_routes():
    try:
        routes = []
        for rule in sorted(app.url_map.iter_rules(), key=lambda r: r.rule):
            methods = ','.join(sorted(m for m in rule.methods if m not in {'HEAD', 'OPTIONS'}))
            routes.append({'rule': rule.rule, 'endpoint': rule.endpoint, 'methods': methods})
        return jsonify({'routes': routes})
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
            
            # Legacy template removed; redirect to main Glass UI
            return redirect(url_for('glass_ui'))
    except Exception as e:
        return f"Error loading map for table {table_name}: {e}"

@app.route('/upload', methods=['GET', 'POST'])
def upload_shapefile():
    """Upload shapefile page and handler"""
    if request.method == 'GET':
        # Legacy template removed; send users to the new Glass UI
        return redirect(url_for('glass_ui'))
    
    # Handle POST request (file upload)
    if 'file' not in request.files:
        return jsonify({'error': 'No file selected'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Only ZIP files are allowed'}), 400
    
    # Create a new engine with AUTOCOMMIT for the upload process
    upload_engine = make_db_engine(isolation_level='AUTOCOMMIT')
    
    try:
        # Generate unique filename
        upload_id = str(uuid.uuid4())
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{upload_id}_{filename}")
        
        # Save uploaded file
        file.save(filepath)
        # Optional dataset name from form
        desired_name = request.form.get('name', '').strip()
        
        # Process the shapefile with the new engine
        result = process_shapefile_upload(filepath, upload_id, desired_name)
        
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
        # Try to rollback any pending transactions
        try:
            with upload_engine.connect() as conn:
                conn.execute(text("ROLLBACK"))
        except:
            pass
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

def process_shapefile_upload(zip_filepath, upload_id, desired_name=None):
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
            
            # Determine table name: prefer provided desired_name, else from filename
            def sanitize_name(name: str) -> str:
                cleaned = ''.join(c if c.isalnum() else '_' for c in name.lower())
                if not cleaned:
                    return ''
                if not cleaned[0].isalpha():
                    cleaned = 'table_' + cleaned
                return cleaned

            base_name = os.path.splitext(shp_file)[0]
            preferred = sanitize_name(desired_name) if desired_name else ''
            table_name = preferred or sanitize_name(base_name)
            if not table_name:
                table_name = 'table_' + upload_id.replace('-', '')[:8]
            
            # Determine a unique table name without provoking transaction-aborting errors
            engine_for_check = make_db_engine(isolation_level='AUTOCOMMIT')
            with engine_for_check.connect() as conn:
                # Use information_schema to check existence safely
                existing_tables = conn.execute(text(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                )).fetchall()
                existing_names = {row[0] for row in existing_tables}

                # If the preferred name exists, add a numeric suffix
                candidate = table_name
                suffix = 1
                while candidate in existing_names:
                    candidate = f"{table_name}_{suffix}"
                    suffix += 1
                table_name = candidate
            
            # Read and import shapefile
            gdf = gpd.read_file(shp_path)
            
            if len(gdf) == 0:
                return {'success': False, 'error': 'Shapefile contains no features'}
            
            # Ensure CRS is WGS84 for web mapping compatibility
            try:
                if gdf.crs is None:
                    # Try to let GeoPandas infer from .prj; if still None, proceed but warn
                    print("[WARN] Uploaded shapefile has no CRS; attempting import as-is. Consider including a .prj file.")
                else:
                    epsg = gdf.crs.to_epsg()
                    if epsg is None:
                        # Non-EPSG CRS: reproject via WGS84 if possible
                        gdf = gdf.to_crs(4326)
                    elif epsg != 4326:
                        gdf = gdf.to_crs(4326)
            except Exception as crs_err:
                print(f"[WARN] CRS handling failed, continuing without reprojection: {crs_err}")

            # Ensure PostGIS is enabled using a separate AUTOCOMMIT engine so we don't
            # poison the main upload transaction if privileges are insufficient
            try:
                ensure_engine = make_db_engine(isolation_level='AUTOCOMMIT')
                with ensure_engine.connect() as ensure_conn:
                    ensure_conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            except Exception as ext_err:
                # Not fatal; import will still proceed and report clearer errors if PostGIS is missing
                print(f"[WARN] Could not ensure PostGIS extension (non-fatal): {ext_err}")

            # Import to PostGIS using a dedicated engine for geometry support
            upload_engine = make_db_engine(isolation_level='READ COMMITTED')
            try:
                with upload_engine.connect() as upload_conn:
                    gdf.to_postgis(
                        name=table_name,
                        con=upload_conn,
                        if_exists='replace',
                        index=False
                    )
            except AttributeError as ae:
                # psycopg3 cursor lacks copy_expert; fallback to WKT temp table then convert
                if 'copy_expert' in str(ae):
                    tmp_table = f"{table_name}__wkt_tmp"
                    df = gdf.drop(columns=['geometry']).copy()
                    df['__geometry_wkt'] = gdf.geometry.to_wkt()
                    with upload_engine.begin() as conn:
                        # Write temp table
                        df.to_sql(tmp_table, conn, if_exists='replace', index=False)
                        # Create final table with geometry from WKT
                        # Build column list excluding wkt helper
                        non_geom_cols = ', '.join([f'"{c}"' for c in df.columns if c != '__geometry_wkt'])
                        select_cols = non_geom_cols + (', ' if non_geom_cols else '') + "ST_GeomFromText(__geometry_wkt, 4326)::geometry AS geometry"
                        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;'))
                        conn.execute(text(f'CREATE TABLE "{table_name}" AS SELECT {select_cols} FROM "{tmp_table}";'))
                        # Add spatial index (optional)
                        conn.execute(text(f'CREATE INDEX IF NOT EXISTS "{table_name}_geom_gix" ON "{table_name}" USING GIST (geometry);'))
                        conn.execute(text(f'DROP TABLE IF EXISTS "{tmp_table}";'))
                else:
                    raise
            except Exception as import_err:
                # Provide clearer hints if PostGIS or permissions are missing
                msg = str(import_err)
                if 'st_geomfromtext' in msg.lower() or 'postgis' in msg.lower():
                    return {
                        'success': False,
                        'error': 'PostGIS extension is not enabled on the database. Please run "CREATE EXTENSION IF NOT EXISTS postgis;" with sufficient privileges and try again.'
                    }
                return {'success': False, 'error': f'Import error: {msg}'}
            
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
                                    record_dict = dict(record)
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
    import os
    # Get port from environment variable (Render sets this automatically)
    port = int(os.environ.get('PORT', 5000))
    
    # Check if running in production
    is_production = os.environ.get('FLASK_ENV') == 'production'
    
    # Run Flask app
    app.run(
        debug=not is_production,  # Disable debug in production
        host='0.0.0.0', 
        port=port,
        use_reloader=False  # Disable auto-reload to prevent upload interruptions
    )
