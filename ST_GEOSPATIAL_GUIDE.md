# Databricks ST Geospatial Functions Guide
## European Climate Risk Pipeline

This guide explains how Databricks ST geospatial functions are integrated with H3 spatial indexing in the European Climate Risk Pipeline for advanced spatial analytics.

---

## 📚 Overview

The pipeline uses **both H3 and ST geospatial functions** for optimal performance:

- **H3 Hexagonal Indexing**: Fast spatial grouping and efficient joins
- **ST Geospatial Functions**: Precise geometric operations, distance calculations, and GIS integration

### Import Statement

All pipelines import Databricks ST functions:

```python
from pyspark.databricks.sql import functions as dbf
```

---

## 🗺️ ST Functions Used in the Pipeline

### 1. **Point Construction**

#### `st_point(x, y)`
Creates a GEOMETRY point from longitude and latitude coordinates.

**Usage in Pipeline:**
```python
# In all pipelines - create point geometry from coordinates
.withColumn("geom_point", dbf.st_point(F.col("longitude"), F.col("latitude")))
```

**Tables with ST Points:**
- `silver_terrain_unified`
- `silver_terrain_derivatives`
- `silver_weather_europe_enriched`
- `gold_flood_risk_scores`
- `gold_drought_risk_scores`

---

### 2. **Geography Construction**

#### `st_geogfromtext(wkt)`
Creates a GEOGRAPHY point from WKT (Well-Known Text) representation for spherical distance calculations.

**Usage in Pipeline:**
```python
# Create GEOGRAPHY point for geodesic calculations
.withColumn("geog_point", dbf.st_geogfromtext(
    F.concat(F.lit("POINT("), F.col("longitude"), F.lit(" "), F.col("latitude"), F.lit(")"))
))
```

**Use Case:** 
- Spherical distance calculations on Earth's surface
- More accurate than Cartesian distances for geographic data

---

### 3. **Buffer Creation**

#### `st_buffer(geom, radius)`
Creates a circular buffer around a geometry with specified radius (in meters for GEOGRAPHY, units of SRID for GEOMETRY).

**Usage in Pipeline:**

**Weather Impact Zones:**
```python
# Create multiple buffer zones for weather impact analysis
.withColumn("buffer_5km", dbf.st_buffer(F.col("geom_point"), 5000.0))
.withColumn("buffer_10km", dbf.st_buffer(F.col("geom_point"), 10000.0))
.withColumn("buffer_25km", dbf.st_buffer(F.col("geom_point"), 25000.0))
```

**Flood Evacuation Zones:**
```python
# Dynamic evacuation zones based on flood risk level
.withColumn("evacuation_zone_radius_m",
    F.when(F.col("flood_risk_category") == "CRITICAL", 5000.0)
    .when(F.col("flood_risk_category") == "HIGH", 3000.0)
    .when(F.col("flood_risk_category") == "MODERATE", 1000.0)
    .otherwise(500.0)
)
.withColumn("evacuation_zone", 
    dbf.st_buffer(F.col("geom_point"), F.col("evacuation_zone_radius_m"))
)
```

**Drought Restriction Zones:**
```python
# Water restriction zones based on drought severity
.withColumn("restriction_zone", 
    dbf.st_buffer(F.col("geom_point"), F.col("restriction_zone_radius_m"))
)
```

**Applications:**
- Evacuation planning
- Impact assessment
- Resource allocation
- Population at risk estimation

---

### 4. **Distance Calculations**

#### `st_distance(geom1, geom2)`
Calculates 2D Cartesian distance between two geometries.

**Usage in Pipeline:**
```python
# Validate spatial join quality - measure distance between matched points
.withColumn("geom_match_distance_m",
    F.when(
        weather.geom_point.isNotNull() & terrain.geom_point.isNotNull(),
        dbf.st_distance(terrain.geom_point, weather.geom_point)
    ).otherwise(0.0)
)
```

**Use Case:**
- Data quality validation
- Spatial join verification
- Proximity analysis

---

### 5. **Spatial Relationships**

#### `st_dwithin(geom1, geom2, distance)`
Returns true if the distance between two geometries is within the specified threshold.

**Usage in Pipeline:**
```python
# Validate H3 spatial joins with distance check (within 30km)
.withColumn("spatial_match_valid",
    F.when(
        weather.geom_point.isNotNull() & terrain.geom_point.isNotNull(),
        dbf.st_dwithin(terrain.geom_point, weather.geom_point, 30000.0)
    ).otherwise(True)
)
.filter(F.col("spatial_match_valid"))
```

**Benefits:**
- Ensures spatial join accuracy
- Filters false matches from H3 cell boundaries
- Quality assurance for risk calculations

---

### 6. **Area Calculations**

#### `st_area(geom)`
Calculates the area of a geometry (in square meters for GEOGRAPHY, square units of SRID for GEOMETRY).

**Usage in Pipeline:**

**Flood Impact Assessment:**
```python
# Calculate area of flood impact zones
.withColumn("impact_zone_1km_area_m2", dbf.st_area(F.col("flood_impact_zone_1km")))
.withColumn("impact_zone_5km_area_m2", dbf.st_area(F.col("flood_impact_zone_5km")))

# Calculate evacuation zone area in square kilometers
.withColumn("evacuation_zone_area_km2", 
    dbf.st_area(F.col("evacuation_zone")) / 1000000.0
)
```

**Drought Impact Assessment:**
```python
# Calculate area affected by drought restrictions
.withColumn("affected_area_km2", 
    dbf.st_area(F.col("restriction_zone")) / 1000000.0
)

# Monitoring areas
.withColumn("monitoring_area_10km_km2", 
    dbf.st_area(F.col("drought_impact_zone_10km")) / 1000000.0
)
```

**Applications:**
- Population density calculations
- Economic impact estimation
- Resource requirement planning
- Insurance risk quantification

---

### 7. **Centroid Calculation**

#### `st_centroid(geom)`
Returns the centroid of a geometry as a point.

**Usage in Pipeline:**
```python
# Calculate centroid of terrain buffer for analysis
.withColumn("buffer_centroid", dbf.st_centroid(F.col("buffer_250m")))
```

**Use Case:**
- Representative point for irregular geometries
- Aggregation anchor points
- Simplification of complex polygons

---

### 8. **Export Functions**

#### `st_asgeojson(geom)` & `st_aswkt(geom)`
Export geometries to standard GIS formats.

**Usage in Pipeline:**

**GeoJSON Export (for web mapping):**
```python
# Export for Leaflet, Mapbox, etc.
.withColumn("location_geojson", dbf.st_asgeojson(F.col("geom_point")))
.withColumn("evacuation_zone_geojson", dbf.st_asgeojson(F.col("evacuation_zone")))
.withColumn("restriction_zone_geojson", dbf.st_asgeojson(F.col("restriction_zone")))
```

**WKT Export (for traditional GIS):**
```python
# Export for QGIS, ArcGIS, PostGIS
.withColumn("location_wkt", dbf.st_aswkt(F.col("geom_point")))
```

**Applications:**
- Web mapping integration
- GIS software compatibility
- External system integration
- Visualization

---

## 🔄 Combined H3 + ST Workflow

### Why Use Both?

1. **H3 for Performance**
   - Fast spatial indexing
   - Efficient grouping and aggregation
   - Approximate spatial joins

2. **ST for Precision**
   - Exact geometric operations
   - Buffer zones and impact areas
   - Distance validation
   - GIS integration

### Example Workflow

```python
# Step 1: Join by H3 (fast approximate join)
terrain.join(weather, on="h3_cell_8", how="left")

# Step 2: Validate with ST (precise verification)
.withColumn("spatial_match_valid",
    dbf.st_dwithin(terrain.geom_point, weather.geom_point, 30000.0)
)
.filter(F.col("spatial_match_valid"))

# Step 3: Create impact zones with ST
.withColumn("impact_zone", dbf.st_buffer(F.col("geom_point"), 5000.0))
.withColumn("impact_area_km2", dbf.st_area(F.col("impact_zone")) / 1000000.0)
```

---

## 📊 Example Queries

### Query 1: Find Overlapping Evacuation Zones

```sql
-- Find cities with overlapping flood evacuation zones
SELECT 
  a.location_name as city_a,
  b.location_name as city_b,
  ST_INTERSECTION(a.evacuation_zone, b.evacuation_zone) as overlap_zone,
  ST_AREA(ST_INTERSECTION(a.evacuation_zone, b.evacuation_zone)) / 1000000.0 as overlap_area_km2
FROM demo_hc.risk_analytics.gold_flood_risk_scores a
INNER JOIN demo_hc.risk_analytics.gold_flood_risk_scores b
  ON a.h3_cell != b.h3_cell
  AND ST_INTERSECTS(a.evacuation_zone, b.evacuation_zone)
WHERE a.flood_risk_category IN ('CRITICAL', 'HIGH')
  AND b.flood_risk_category IN ('CRITICAL', 'HIGH');
```

### Query 2: Calculate Population at Risk (with census data)

```sql
-- Estimate population within flood evacuation zones
-- Assumes a census_data table with population and geometry
SELECT 
  f.location_name,
  f.flood_risk_category,
  f.evacuation_zone_area_km2,
  SUM(c.population) as estimated_population_at_risk
FROM demo_hc.risk_analytics.gold_flood_risk_scores f
LEFT JOIN census_data c
  ON ST_INTERSECTS(f.evacuation_zone, c.geom_boundary)
WHERE f.immediate_alert = true
GROUP BY f.location_name, f.flood_risk_category, f.evacuation_zone_area_km2
ORDER BY estimated_population_at_risk DESC;
```

### Query 3: Find Nearest Weather Station

```sql
-- For each high-risk location, find the nearest weather monitoring station
WITH stations AS (
  SELECT DISTINCT
    location_name,
    geom_point,
    country_code
  FROM demo_hc.processed_data.silver_weather_europe_enriched
),
risk_locations AS (
  SELECT
    h3_cell,
    location_name as risk_location,
    geom_point,
    flood_risk_score
  FROM demo_hc.risk_analytics.gold_flood_risk_scores
  WHERE flood_risk_score >= 60
)
SELECT
  r.risk_location,
  s.location_name as nearest_station,
  ST_DISTANCE(r.geom_point, s.geom_point) / 1000.0 as distance_km,
  r.flood_risk_score
FROM risk_locations r
CROSS JOIN stations s
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.h3_cell ORDER BY ST_DISTANCE(r.geom_point, s.geom_point)) = 1
ORDER BY r.flood_risk_score DESC;
```

### Query 4: Buffer Analysis - Areas Within 10km of High Risk

```sql
-- Find all H3 cells within 10km of CRITICAL flood risk areas
SELECT 
  t.h3_cell_8,
  t.avg_elevation_m,
  COUNT(f.h3_cell) as nearby_critical_flood_zones,
  MIN(ST_DISTANCE(t.geom_point, f.geom_point)) / 1000.0 as distance_to_nearest_critical_km
FROM demo_hc.processed_data.silver_terrain_unified t
CROSS JOIN (
  SELECT h3_cell, geom_point 
  FROM demo_hc.risk_analytics.gold_flood_risk_scores
  WHERE flood_risk_category = 'CRITICAL'
) f
WHERE ST_DWITHIN(t.geom_point, f.geom_point, 10000.0)
GROUP BY t.h3_cell_8, t.avg_elevation_m
ORDER BY nearby_critical_flood_zones DESC;
```

### Query 5: Export GeoJSON for Web Mapping

```sql
-- Export current high-risk flood locations as GeoJSON for Mapbox/Leaflet
SELECT 
  location_name,
  country_code,
  flood_risk_score,
  flood_risk_category,
  location_geojson as point_geojson,
  evacuation_zone_geojson as zone_geojson,
  evacuation_zone_area_km2,
  recommended_action,
  observation_time
FROM demo_hc.risk_analytics.gold_flood_high_risk_alerts
WHERE immediate_alert = true
ORDER BY flood_risk_score DESC;
```

### Query 6: Spatial Aggregation by Administrative Boundaries

```sql
-- Aggregate drought risk by administrative regions (assuming admin_boundaries table)
SELECT 
  a.region_name,
  a.country_code,
  COUNT(d.h3_cell) as drought_affected_cells,
  AVG(d.drought_risk_score) as avg_drought_risk,
  SUM(d.affected_area_km2) as total_affected_area_km2,
  ST_UNION_AGG(d.restriction_zone) as combined_restriction_zone,
  ST_AREA(ST_UNION_AGG(d.restriction_zone)) / 1000000.0 as combined_zone_area_km2
FROM admin_boundaries a
INNER JOIN demo_hc.risk_analytics.gold_drought_risk_scores d
  ON ST_CONTAINS(a.boundary_geom, d.geom_point)
WHERE d.drought_risk_category IN ('EXTREME', 'SEVERE')
GROUP BY a.region_name, a.country_code
ORDER BY avg_drought_risk DESC;
```

---

## 🎨 Visualization Integration

### Mapbox GL JS Example

```javascript
// Fetch GeoJSON from Databricks SQL endpoint
fetch('/api/flood-alerts')
  .then(response => response.json())
  .then(data => {
    // Add point layer
    map.addSource('flood-alerts', {
      type: 'geojson',
      data: {
        type: 'FeatureCollection',
        features: data.map(row => JSON.parse(row.location_geojson))
      }
    });
    
    // Add evacuation zone layer
    map.addSource('evacuation-zones', {
      type: 'geojson',
      data: {
        type: 'FeatureCollection',
        features: data.map(row => JSON.parse(row.evacuation_zone_geojson))
      }
    });
    
    // Style by risk category
    map.addLayer({
      id: 'zones',
      type: 'fill',
      source: 'evacuation-zones',
      paint: {
        'fill-color': [
          'match',
          ['get', 'risk_category'],
          'CRITICAL', '#d73027',
          'HIGH', '#fc8d59',
          'MODERATE', '#fee08b',
          '#cccccc'
        ],
        'fill-opacity': 0.4
      }
    });
  });
```

### Python Folium Example

```python
import folium
from folium import plugins

# Query Databricks
flood_alerts = spark.sql("""
  SELECT 
    location_name,
    flood_risk_score,
    ST_Y(geom_point) as latitude,
    ST_X(geom_point) as longitude,
    evacuation_zone_geojson
  FROM demo_hc.risk_analytics.gold_flood_high_risk_alerts
  WHERE immediate_alert = true
""").toPandas()

# Create map
m = folium.Map(location=[50.0, 10.0], zoom_start=5)

# Add markers and zones
for _, row in flood_alerts.iterrows():
    # Point marker
    folium.Marker(
        [row['latitude'], row['longitude']],
        popup=f"{row['location_name']}: {row['flood_risk_score']:.1f}",
        icon=folium.Icon(color='red', icon='warning')
    ).add_to(m)
    
    # Evacuation zone
    folium.GeoJson(
        row['evacuation_zone_geojson'],
        style_function=lambda x: {
            'fillColor': '#ff0000',
            'color': '#ff0000',
            'fillOpacity': 0.2
        }
    ).add_to(m)

m.save('flood_risk_map.html')
```

---

## 🔧 Performance Optimization

### Best Practices

1. **Use H3 for Initial Filtering**
   ```python
   # Good: Filter by H3 first, then apply ST functions
   .filter(F.col("h3_cell_6") == target_region)
   .withColumn("precise_distance", dbf.st_distance(...))
   ```

2. **Create Spatial Indices**
   ```sql
   -- Z-order by geometry bounds for faster spatial queries
   OPTIMIZE demo_hc.risk_analytics.gold_flood_risk_scores
   ZORDER BY (h3_cell_8, ST_XMIN(geom_point), ST_YMIN(geom_point));
   ```

3. **Materialize Complex Geometries**
   ```python
   # Materialize expensive ST operations
   .withColumn("evacuation_zone", dbf.st_buffer(...))
   .cache()  # If reusing multiple times
   ```

4. **Use Appropriate Data Types**
   - Use `GEOMETRY` for planar calculations (faster)
   - Use `GEOGRAPHY` for spherical accuracy (more accurate for global data)

---

## 📚 Additional ST Functions Available

The pipeline can be extended with these ST functions:

### Topological Relationships
- `st_contains(geom1, geom2)` - Check containment
- `st_intersects(geom1, geom2)` - Check intersection
- `st_within(geom1, geom2)` - Check if within
- `st_touches(geom1, geom2)` - Check if touching
- `st_covers(geom1, geom2)` - Check coverage

### Overlay Operations
- `st_intersection(geom1, geom2)` - Get intersection geometry
- `st_union(geom1, geom2)` - Get union geometry
- `st_difference(geom1, geom2)` - Get difference geometry

### Geometry Processing
- `st_convexhull(geom)` - Get convex hull
- `st_simplify(geom, tolerance)` - Simplify geometry
- `st_envelope(geom)` - Get bounding box

---

## 🚀 Future Enhancements

### Planned ST Function Integrations

1. **Flood Flow Modeling**
   - Use `st_intersection` with terrain and river networks
   - Calculate downstream impact zones

2. **Drought Spread Analysis**
   - Use `st_union_agg` to merge adjacent drought zones
   - Track expansion over time

3. **Infrastructure Vulnerability**
   - Use `st_contains` to identify buildings in risk zones
   - Calculate road network disruption

4. **Multi-Hazard Analysis**
   - Combine flood and drought zones with `st_union`
   - Identify compound risk areas

---

## 📖 References

- [Databricks ST Functions Documentation](https://docs.databricks.com/sql/language-manual/functions/alphabetical-list-st-geospatial-functions.html)
- [H3 Spatial Indexing](https://h3geo.org/)
- [OGC Simple Features Specification](https://www.ogc.org/standards/sfa)
- [GeoJSON Specification](https://geojson.org/)

---

**Last Updated:** 2025-11-12  
**Version:** 1.0.0  
**Pipeline:** European Climate Risk Assessment

