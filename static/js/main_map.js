// Global variable for the map
let map;

/**
 * Initializes the Google Map and sets up event listeners.
 * This function is called by the Google Maps API script callback.
 */
function initMap() {
    const mapElement = document.getElementById('map');
    const tablesData = mapElement.dataset.tables;
    const tables = JSON.parse(tablesData);

    // Initialize map
    map = new google.maps.Map(mapElement, {
        center: { lat: 20, lng: 0 }, // A more central starting point
        zoom: 3,
        mapTypeId: 'roadmap',
        mapTypeControl: false,
        streetViewControl: false,
        fullscreenControl: false,
    });

    // Populate the dropdown menu
    const tableSelect = document.getElementById('table-select');
    tables.forEach(tableName => {
        const option = document.createElement('option');
        option.value = tableName;
        option.textContent = tableName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()); // Prettify name
        tableSelect.appendChild(option);
    });

    // Add event listener for dropdown selection
    tableSelect.addEventListener('change', () => {
        const selectedTable = tableSelect.value;
        if (selectedTable) {
            loadTableData(selectedTable);
        }
    });

    // Add event listener for layer toggle button
    const layerToggleBtn = document.getElementById('layer-toggle-btn');
    layerToggleBtn.addEventListener('click', () => {
        toggleMapLayer();
    });
}

/**
 * Loads GeoJSON data for the selected table and fits the map to its bounds.
 * @param {string} tableName The name of the table to load.
 */
function loadTableData(tableName) {
    // Clear any existing data from the map
    map.data.forEach(feature => {
        map.data.remove(feature);
    });

    const apiUrl = `/api/geojson/${tableName}`;

    // Show a loading indicator (optional, but good UX)
    const tableSelect = document.getElementById('table-select');
    tableSelect.disabled = true;

    map.data.loadGeoJson(apiUrl, null, (features) => {
        if (features.length > 0) {
            const bounds = new google.maps.LatLngBounds();
            features.forEach(feature => {
                processPoints(feature.getGeometry(), bounds.extend, bounds);
            });
            map.fitBounds(bounds);
        }
        tableSelect.disabled = false; // Re-enable dropdown
    });
}

/**
 * Toggles the map layer between roadmap and satellite views.
 */
function toggleMapLayer() {
    const layerToggleBtn = document.getElementById('layer-toggle-btn');
    if (map.getMapTypeId() === 'roadmap') {
        map.setMapTypeId('satellite');
        layerToggleBtn.innerHTML = '<i class="fas fa-map"></i> Map';
    } else {
        map.setMapTypeId('roadmap');
        layerToggleBtn.innerHTML = '<i class="fas fa-satellite"></i> Satellite';
    }
}

/**
 * Helper function to recursively process geometries for bounds calculation.
 * This is necessary to handle complex shapes like MultiPolygons.
 * @param {google.maps.Data.Geometry} geometry The geometry to process.
 * @param {Function} callback The function to call for each point.
 * @param {*} thisArg The `this` context for the callback.
 */
function processPoints(geometry, callback, thisArg) {
    if (geometry instanceof google.maps.Data.Point) {
        callback.call(thisArg, geometry.get());
    } else if (geometry instanceof google.maps.Data.LineString || geometry instanceof google.maps.Data.MultiPoint) {
        geometry.getArray().forEach(p => callback.call(thisArg, p));
    } else if (geometry instanceof google.maps.Data.Polygon || geometry instanceof google.maps.Data.MultiLineString) {
        geometry.getArray().forEach(ring => ring.getArray().forEach(p => callback.call(thisArg, p)));
    } else if (geometry instanceof google.maps.Data.MultiPolygon) {
        geometry.getArray().forEach(poly => poly.getArray().forEach(ring => ring.getArray().forEach(p => callback.call(thisArg, p))));
    }
}

// Assign initMap to the window object to make it accessible by the Google Maps API callback.
window.initMap = initMap;
