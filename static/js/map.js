let map;

function initMap() {
    const mapElement = document.getElementById('map');
    const tableName = mapElement.dataset.tableName;
    const filterQuery = mapElement.dataset.filterQuery;
    const filterType = mapElement.dataset.filterType;

    // Initialize map
    map = new google.maps.Map(mapElement, {
        center: { lat: 0, lng: 0 },
        zoom: 2,
        mapTypeId: 'roadmap', // Default map type
        mapTypeControl: false, // Disable default map type control
        streetViewControl: false // Disable street view
    });

    // Custom layer control
    const toggleLayersBtn = document.getElementById('toggle-layers');
    toggleLayersBtn.addEventListener('click', () => {
        const currentMapTypeId = map.getMapTypeId();
        if (currentMapTypeId === 'roadmap') {
            map.setMapTypeId('satellite');
            toggleLayersBtn.innerHTML = '<i class="fas fa-map"></i> Map View';
        } else {
            map.setMapTypeId('roadmap');
            toggleLayersBtn.innerHTML = '<i class="fas fa-layer-group"></i> Layers';
        }
    });

    // Filter parameters
    let isFiltered = filterType === 'search' && filterQuery !== '';

    // Function to load GeoJSON data
    function loadMapData(useFilter = isFiltered) {
        const apiUrl = useFilter && filterQuery ?
            `/api/geojson/${tableName}/filtered?q=${encodeURIComponent(filterQuery)}` :
            `/api/geojson/${tableName}`;

        // Clear existing data
        map.data.forEach(feature => {
            map.data.remove(feature);
        });

        document.getElementById('map-status').innerHTML = '<span class="text-warning">Loading...</span>';

        // Load GeoJSON data
        map.data.loadGeoJson(apiUrl, null, (features) => {
            if (features.length === 0) {
                document.getElementById('map-status').innerHTML = '<span class="text-warning">No features found</span>';
                return;
            }

            const statusText = useFilter ?
                `<span class="text-success">Loaded ${features.length} filtered features</span>` :
                `<span class="text-success">Loaded ${features.length} features</span>`;
            document.getElementById('map-status').innerHTML = statusText;

            // Fit map to data bounds
            const bounds = new google.maps.LatLngBounds();
            features.forEach(feature => {
                processPoints(feature.getGeometry(), bounds.extend, bounds);
            });
            map.fitBounds(bounds);
        });
    }

    // Helper function to process geometries for bounds calculation
    function processPoints(geometry, callback, thisArg) {
        if (geometry instanceof google.maps.LatLng) {
            callback.call(thisArg, geometry);
        } else if (geometry instanceof google.maps.Data.Point) {
            callback.call(thisArg, geometry.get());
        } else {
            geometry.getArray().forEach(g => {
                processPoints(g, callback, thisArg);
            });
        }
    }

    // Initial data load
    loadMapData();

    // Filter toggle button event handler
    const toggleFilterBtn = document.getElementById('toggle-filter-btn');
    if (toggleFilterBtn) {
        toggleFilterBtn.addEventListener('click', function() {
            isFiltered = !isFiltered;

            // Update button text and icon
            const filterIcon = document.getElementById('filter-icon');
            const filterText = document.getElementById('filter-text');

            if (isFiltered) {
                filterIcon.className = 'fas fa-eye-slash';
                filterText.textContent = 'Show All';
            } else {
                filterIcon.className = 'fas fa-eye';
                filterText.textContent = 'Show Filtered';
            }

            // Reload map data with new filter state
            loadMapData(isFiltered);
        });
    }

    // Fit bounds button
    document.getElementById('fit-bounds-btn').addEventListener('click', () => {
        const bounds = new google.maps.LatLngBounds();
        map.data.forEach(feature => {
            processPoints(feature.getGeometry(), bounds.extend, bounds);
        });
        map.fitBounds(bounds);
    });

    // Show feature information on click
    map.data.addListener('click', event => {
        showFeatureInfo(event.feature);
    });

    // Style features
    map.data.setStyle(feature => {
        const geomType = feature.getGeometry().getType();
        let style = {
            clickable: true,
            strokeColor: '#3388ff',
            strokeOpacity: 1,
            strokeWeight: 2,
            fillColor: '#3388ff',
            fillOpacity: 0.3
        };

        if (geomType === 'Point' || geomType === 'MultiPoint') {
            style.icon = {
                path: google.maps.SymbolPath.CIRCLE,
                scale: 5,
                fillColor: '#ff7800',
                fillOpacity: 0.8,
                strokeColor: '#000',
                strokeWeight: 1
            };
        } else if (geomType === 'LineString' || geomType === 'MultiLineString') {
            style.strokeColor = '#ff7800';
            style.strokeOpacity = 0.8;
            style.strokeWeight = 3;
        }

        return style;
    });
}

function showFeatureInfo(feature) {
    const featureInfo = document.getElementById('feature-info');
    const featureDetails = document.getElementById('feature-details');

    let content = '<div class="row">';
    content += '<div class="col-md-6">';
    content += '<h6><i class="fas fa-shapes"></i> Geometry</h6>';
    content += `<p><strong>Type:</strong> ${feature.getGeometry().getType()}</p>`;

    content += '</div>';
    content += '<div class="col-md-6">';
    content += '<h6><i class="fas fa-tags"></i> Properties</h6>';

    const properties = {};
    feature.forEachProperty((value, key) => {
        properties[key] = value;
    });

    if (Object.keys(properties).length > 0) {
        for (const [key, value] of Object.entries(properties)) {
            if (value !== null && value !== undefined) {
                content += `<p><strong>${key}:</strong> ${value}</p>`;
            }
        }
    } else {
        content += '<p class="text-muted">No properties available</p>';
    }

    content += '</div>';
    content += '</div>';

    featureDetails.innerHTML = content;
    featureInfo.style.display = 'block';

    // Scroll to feature info
    featureInfo.scrollIntoView({ behavior: 'smooth' });
}

// Assign initMap to window to make it accessible by Google Maps API callback
window.initMap = initMap;
