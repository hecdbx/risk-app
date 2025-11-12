# Databricks ST Functions Integration - Quick Reference

## 🎯 What Was Added

All four pipelines now include **Databricks ST geospatial functions** alongside H3 hexagonal indexing for powerful spatial analytics.

---

## 📦 Import Statement

```python
from pyspark.databricks.sql import functions as dbf
```

---

## 🗺️ ST Functions by Pipeline

### 1. Terrain Ingestion (`01_terrain_dem_ingestion.py`)

| Function | Usage | Purpose |
|----------|-------|---------|
| `st_point(lon, lat)` | Create point geometry | Convert coordinates to GEOMETRY type |
| `st_geogfromtext(wkt)` | Create GEOGRAPHY point | Spherical distance calculations |
| `st_buffer(geom, 250)` | 250m buffer zone | Neighborhood terrain analysis |
| `st_centroid(geom)` | Buffer centroid | Representative point calculation |

**Example:**
```python
.withColumn("geom_point", dbf.st_point(F.col("longitude"), F.col("latitude")))
.withColumn("buffer_250m", dbf.st_buffer(F.col("geom_point"), 250.0))
```

---

### 2. Weather Ingestion (`02_accuweather_europe_ingestion.py`)

| Function | Usage | Purpose |
|----------|-------|---------|
| `st_point(lon, lat)` | Create point geometry | Weather station location |
| `st_geogfromtext(wkt)` | Create GEOGRAPHY point | Geodesic calculations |
| `st_buffer(geom, radius)` | 5km, 10km, 25km buffers | Weather impact zones |

**Example:**
```python
.withColumn("buffer_5km", dbf.st_buffer(F.col("geom_point"), 5000.0))
.withColumn("buffer_10km", dbf.st_buffer(F.col("geom_point"), 10000.0))
.withColumn("buffer_25km", dbf.st_buffer(F.col("geom_point"), 25000.0))
```

---

### 3. Flood Risk Transformation (`03_flood_risk_transformation.py`)

| Function | Usage | Purpose |
|----------|-------|---------|
| `st_dwithin(g1, g2, 30000)` | 30km distance check | Spatial join validation |
| `st_distance(g1, g2)` | Point-to-point distance | Data quality verification |
| `st_buffer(geom, radius)` | Dynamic evacuation zones | Risk-based buffer zones (500m-5km) |
| `st_area(geom)` | Zone area calculation | Impact assessment (km²) |
| `st_asgeojson(geom)` | GeoJSON export | Web mapping integration |

**Example - Dynamic Evacuation Zones:**
```python
.withColumn("evacuation_zone_radius_m",
    F.when(F.col("flood_risk_category") == "CRITICAL", 5000.0)
    .when(F.col("flood_risk_category") == "HIGH", 3000.0)
    .when(F.col("flood_risk_category") == "MODERATE", 1000.0)
    .otherwise(500.0)
)
.withColumn("evacuation_zone", 
    dbf.st_buffer(F.col("geom_point"), F.col("evacuation_zone_radius_m"))
)
.withColumn("evacuation_zone_area_km2", 
    dbf.st_area(F.col("evacuation_zone")) / 1000000.0
)
```

---

### 4. Drought Risk Transformation (`04_drought_risk_transformation.py`)

| Function | Usage | Purpose |
|----------|-------|---------|
| `st_distance(g1, g2)` | Quality check | Validate spatial matching |
| `st_buffer(geom, radius)` | Restriction zones | Water restriction areas (5km-25km) |
| `st_area(geom)` | Affected area | Calculate km² impacted |
| `st_asgeojson(geom)` | GeoJSON export | Web mapping |
| `st_aswkt(geom)` | WKT export | GIS integration (QGIS, ArcGIS) |

**Example - Water Restriction Zones:**
```python
.withColumn("restriction_zone_radius_m",
    F.when(F.col("drought_risk_category") == "EXTREME", 25000.0)
    .when(F.col("drought_risk_category") == "SEVERE", 15000.0)
    .when(F.col("drought_risk_category") == "MODERATE", 10000.0)
    .otherwise(5000.0)
)
.withColumn("restriction_zone", 
    dbf.st_buffer(F.col("geom_point"), F.col("restriction_zone_radius_m"))
)
.withColumn("affected_area_km2", 
    dbf.st_area(F.col("restriction_zone")) / 1000000.0
)
```

---

## 🔑 Key ST Functions Used

### Constructor Functions
- ✅ `st_point(x, y)` - Create point GEOMETRY from coordinates
- ✅ `st_geogfromtext(wkt)` - Create GEOGRAPHY from WKT string

### Measurement Functions
- ✅ `st_distance(geom1, geom2)` - Calculate distance between geometries
- ✅ `st_area(geom)` - Calculate area in square meters
- ✅ `st_dwithin(geom1, geom2, distance)` - Check if within distance

### Geometry Processing
- ✅ `st_buffer(geom, radius)` - Create buffer zone around geometry
- ✅ `st_centroid(geom)` - Calculate centroid point

### Export Functions
- ✅ `st_asgeojson(geom)` - Export as GeoJSON (for web maps)
- ✅ `st_aswkt(geom)` - Export as WKT (for GIS tools)

---

## 📊 New Columns Created

### All Risk Tables Now Include:

| Column Name | Type | Description |
|-------------|------|-------------|
| `geom_point` | GEOMETRY | Point geometry (lon, lat) |
| `geog_point` | GEOGRAPHY | Geography point for spherical calculations |
| `*_zone` | GEOMETRY | Buffer zones (evacuation, restriction, impact) |
| `*_area_km2` | DOUBLE | Area calculations in square kilometers |
| `*_geojson` | STRING | GeoJSON representation for web mapping |
| `*_wkt` | STRING | WKT representation for GIS tools |
| `geom_match_distance_m` | DOUBLE | Quality check distance between matched points |

---

## 🎯 Use Cases Enabled

### 1. Spatial Join Validation
```sql
-- Ensure terrain and weather data are properly matched
WHERE ST_DWITHIN(terrain.geom_point, weather.geom_point, 30000.0)
```

### 2. Dynamic Risk Zones
```sql
-- Create evacuation zones that scale with risk level
SELECT 
  location_name,
  flood_risk_category,
  ST_BUFFER(geom_point, 
    CASE flood_risk_category 
      WHEN 'CRITICAL' THEN 5000
      WHEN 'HIGH' THEN 3000
      ELSE 1000
    END) as evacuation_zone
```

### 3. Impact Area Calculation
```sql
-- Calculate total area affected by high risk
SELECT 
  country_code,
  SUM(ST_AREA(evacuation_zone)) / 1000000.0 as total_affected_km2
FROM gold_flood_risk_scores
WHERE flood_risk_category IN ('CRITICAL', 'HIGH')
GROUP BY country_code
```

### 4. Proximity Analysis
```sql
-- Find all locations within 50km of Paris
SELECT 
  location_name,
  flood_risk_score,
  ST_DISTANCE(geom_point, ST_POINT(2.3522, 48.8566)) / 1000.0 as distance_km
FROM gold_flood_risk_scores
WHERE ST_DWITHIN(geom_point, ST_POINT(2.3522, 48.8566), 50000.0)
ORDER BY distance_km
```

### 5. GeoJSON Export for Web Mapping
```sql
-- Export high-risk locations as GeoJSON
SELECT 
  location_name,
  flood_risk_score,
  location_geojson,
  evacuation_zone_geojson
FROM gold_flood_high_risk_alerts
WHERE immediate_alert = true
```

---

## 🚀 Performance Benefits

### Why H3 + ST Together?

1. **H3 for Speed**
   - Fast initial filtering
   - Efficient spatial grouping
   - Approximate joins (milliseconds)

2. **ST for Precision**
   - Exact geometric operations
   - Accurate distance calculations
   - Buffer zone creation
   - GIS interoperability

### Optimized Query Pattern

```python
# Step 1: H3 join (fast)
terrain.join(weather, on="h3_cell_8")

# Step 2: ST validation (precise)
.withColumn("valid", dbf.st_dwithin(t.geom_point, w.geom_point, 30000))
.filter(F.col("valid"))

# Step 3: ST analysis
.withColumn("impact_zone", dbf.st_buffer(F.col("geom_point"), 5000))
```

---

## 📈 Tables with ST Functions

### All tables now include spatial geometries:

**Silver Layer:**
- `silver_terrain_unified` - ✅ `geom_point`, `geog_point`
- `silver_terrain_derivatives` - ✅ `geom_point`, `buffer_250m`, `buffer_centroid`
- `silver_weather_europe_enriched` - ✅ `geom_point`, `buffer_5km/10km/25km`
- `silver_flood_terrain_weather_combined` - ✅ All geometries + impact zones
- `silver_drought_terrain_weather_combined` - ✅ All geometries + monitoring zones

**Gold Layer:**
- `gold_flood_risk_scores` - ✅ Evacuation zones + GeoJSON export
- `gold_drought_risk_scores` - ✅ Restriction zones + WKT/GeoJSON export
- `gold_flood_high_risk_alerts` - ✅ Alert geometries for mapping
- `gold_drought_high_risk_alerts` - ✅ Alert geometries for GIS

---

## 🔧 Query Examples

### Example 1: Find Overlapping Zones
```sql
SELECT 
  a.location_name,
  b.location_name,
  ST_AREA(ST_INTERSECTION(a.evacuation_zone, b.evacuation_zone)) / 1000000.0 as overlap_km2
FROM gold_flood_risk_scores a
JOIN gold_flood_risk_scores b 
  ON ST_INTERSECTS(a.evacuation_zone, b.evacuation_zone)
  AND a.h3_cell < b.h3_cell
WHERE overlap_km2 > 1.0
```

### Example 2: Population at Risk
```sql
-- Requires census data with geometries
SELECT 
  f.location_name,
  SUM(c.population) as population_in_zone
FROM gold_flood_risk_scores f
JOIN census_data c
  ON ST_CONTAINS(f.evacuation_zone, c.centroid)
WHERE f.flood_risk_category = 'CRITICAL'
GROUP BY f.location_name
```

### Example 3: Export for Mapbox
```sql
SELECT 
  location_name,
  flood_risk_score,
  location_geojson,
  evacuation_zone_geojson
FROM gold_flood_high_risk_alerts
WHERE immediate_alert = true
```

---

## 📝 Migration Guide

### Before (H3 only):
```python
.withColumn("h3_cell_8", F.expr("h3_latlng_to_cell_string(latitude, longitude, 8)"))
# Basic spatial grouping only
```

### After (H3 + ST):
```python
# Keep H3 for performance
.withColumn("h3_cell_8", F.expr("h3_latlng_to_cell_string(latitude, longitude, 8)"))

# Add ST for precision
.withColumn("geom_point", dbf.st_point(F.col("longitude"), F.col("latitude")))
.withColumn("buffer_zone", dbf.st_buffer(F.col("geom_point"), 5000.0))
.withColumn("zone_area_km2", dbf.st_area(F.col("buffer_zone")) / 1000000.0)
.withColumn("location_geojson", dbf.st_asgeojson(F.col("geom_point")))
```

---

## 🎓 Learn More

- **Full Guide:** See `ST_GEOSPATIAL_GUIDE.md` for comprehensive documentation
- **Databricks Docs:** [ST Functions Reference](https://docs.databricks.com/sql/language-manual/functions/alphabetical-list-st-geospatial-functions.html)
- **OGC Standards:** [Simple Features Specification](https://www.ogc.org/standards/sfa)

---

## ✅ Benefits Summary

| Feature | H3 Only | H3 + ST Functions |
|---------|---------|-------------------|
| **Spatial Indexing** | ✅ Fast | ✅ Fast |
| **Exact Distance** | ❌ Approximate | ✅ Precise |
| **Buffer Zones** | ❌ Not available | ✅ Yes |
| **Area Calculation** | ❌ Approximate | ✅ Exact |
| **GIS Integration** | ❌ Limited | ✅ Full (GeoJSON, WKT) |
| **Spatial Validation** | ❌ Cell-based | ✅ Distance-based |
| **Web Mapping** | ⚠️ Coordinates only | ✅ Full geometries |
| **Impact Assessment** | ⚠️ Limited | ✅ Comprehensive |

---

**Status:** ✅ Integrated across all pipelines  
**Version:** 1.0.0  
**Last Updated:** 2025-11-12

