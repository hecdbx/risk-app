# Catalog Name Change Summary

## Change Details

**Previous Catalog Name:** `european_risk_catalog`  
**New Catalog Name:** `demo_hc`  
**Date:** 2025-11-12

---

## Files Updated

✅ **11 files** successfully updated with the new catalog name:

### 1. Unity Catalog Setup
- ✅ `unity_catalog_setup.sql` - All SQL statements updated

### 2. Orchestration
- ✅ `workflow_orchestrator.py` - Default configuration and deployment messages

### 3. Configuration
- ✅ `config/european_data_sources.yaml` - Unity Catalog configuration section

### 4. Pipeline Files
- ✅ `pipelines/01_terrain_dem_ingestion.py` - Documentation and references
- ✅ `pipelines/02_accuweather_europe_ingestion.py` - Documentation and references
- ✅ `pipelines/03_flood_risk_transformation.py` - Documentation and references
- ✅ `pipelines/04_drought_risk_transformation.py` - Documentation and references

### 5. Documentation
- ✅ `README.md` - All query examples and references
- ✅ `DEPLOYMENT_GUIDE.md` - All deployment instructions
- ✅ `PROJECT_SUMMARY.md` - All catalog references
- ✅ `ST_GEOSPATIAL_GUIDE.md` - All SQL examples

---

## Schema Structure (Unchanged)

The catalog structure remains the same, only the name changed:

```
demo_hc                          (previously: european_risk_catalog)
├── raw_data                     (Bronze Layer)
│   ├── bronze_terrain_*
│   └── bronze_accuweather_*
├── processed_data               (Silver Layer)
│   ├── silver_terrain_*
│   └── silver_weather_*
└── risk_analytics               (Gold Layer)
    ├── gold_flood_risk_*
    └── gold_drought_risk_*
```

---

## SQL Query Updates

### Before:
```sql
SELECT * FROM european_risk_catalog.risk_analytics.gold_flood_risk_scores;
```

### After:
```sql
SELECT * FROM demo_hc.risk_analytics.gold_flood_risk_scores;
```

---

## Python Code Updates

### Before:
```python
orchestrator = EuropeanRiskPipelineOrchestrator()
# Default catalog: "european_risk_catalog"
```

### After:
```python
orchestrator = EuropeanRiskPipelineOrchestrator()
# Default catalog: "demo_hc"
```

---

## Unity Catalog Commands

### Create Catalog:
```sql
CREATE CATALOG IF NOT EXISTS demo_hc
COMMENT 'European Climate Risk Data - Flood and Drought Risk Assessment for Insurance Applications';

USE CATALOG demo_hc;
```

### Create Schemas:
```sql
CREATE SCHEMA IF NOT EXISTS demo_hc.raw_data;
CREATE SCHEMA IF NOT EXISTS demo_hc.processed_data;
CREATE SCHEMA IF NOT EXISTS demo_hc.risk_analytics;
```

---

## Deployment Impact

### No Breaking Changes
- All table names remain the same
- Schema names remain the same (raw_data, processed_data, risk_analytics)
- Only the catalog prefix changes
- Pipeline logic unchanged

### What You Need to Do

1. **If catalog already exists:**
   ```sql
   -- Rename existing catalog (if supported) or recreate
   DROP CATALOG IF EXISTS european_risk_catalog CASCADE;
   ```

2. **Run Unity Catalog setup:**
   ```sql
   -- Run the updated unity_catalog_setup.sql file
   source /Workspace/Shared/risk_app/unity_catalog_setup.sql
   ```

3. **Redeploy pipelines:**
   ```python
   from workflow_orchestrator import EuropeanRiskPipelineOrchestrator
   
   orchestrator = EuropeanRiskPipelineOrchestrator()
   orchestrator.deploy_all_pipelines()
   ```

---

## Full Table References Updated

All references now use `demo_hc` prefix:

### Bronze Tables (raw_data schema):
- `demo_hc.raw_data.bronze_terrain_copernicus_dem`
- `demo_hc.raw_data.bronze_terrain_eea_elevation`
- `demo_hc.raw_data.bronze_terrain_opengeohub`
- `demo_hc.raw_data.bronze_terrain_geoharmonizer`
- `demo_hc.raw_data.bronze_accuweather_europe_current`
- `demo_hc.raw_data.bronze_accuweather_europe_forecast`

### Silver Tables (processed_data schema):
- `demo_hc.processed_data.silver_terrain_unified`
- `demo_hc.processed_data.silver_terrain_derivatives`
- `demo_hc.processed_data.silver_terrain_opengeohub_layers`
- `demo_hc.processed_data.silver_weather_europe_enriched`
- `demo_hc.processed_data.silver_flood_terrain_weather_combined`
- `demo_hc.processed_data.silver_drought_terrain_weather_combined`
- `demo_hc.processed_data.silver_drought_precipitation_history`

### Gold Tables (risk_analytics schema):
- `demo_hc.risk_analytics.gold_terrain_features_flood_risk`
- `demo_hc.risk_analytics.gold_terrain_features_drought_risk`
- `demo_hc.risk_analytics.gold_weather_features_flood_risk`
- `demo_hc.risk_analytics.gold_weather_features_drought_risk`
- `demo_hc.risk_analytics.gold_flood_risk_scores`
- `demo_hc.risk_analytics.gold_flood_risk_regional_summary`
- `demo_hc.risk_analytics.gold_flood_risk_time_series`
- `demo_hc.risk_analytics.gold_flood_high_risk_alerts`
- `demo_hc.risk_analytics.gold_flood_risk_quality_metrics`
- `demo_hc.risk_analytics.gold_drought_indices`
- `demo_hc.risk_analytics.gold_drought_risk_scores`
- `demo_hc.risk_analytics.gold_drought_risk_regional_summary`
- `demo_hc.risk_analytics.gold_drought_risk_time_series`
- `demo_hc.risk_analytics.gold_drought_high_risk_alerts`
- `demo_hc.risk_analytics.gold_drought_risk_quality_metrics`

---

## Example Queries with New Catalog Name

### Query Flood Risk:
```sql
SELECT 
  location_name,
  flood_risk_score,
  flood_risk_category,
  evacuation_zone_area_km2
FROM demo_hc.risk_analytics.gold_flood_risk_scores
WHERE country_code = 'FR'
ORDER BY flood_risk_score DESC;
```

### Query Drought Risk:
```sql
SELECT 
  location_name,
  drought_risk_score,
  drought_risk_category,
  affected_area_km2
FROM demo_hc.risk_analytics.gold_drought_risk_scores
WHERE drought_risk_category IN ('EXTREME', 'SEVERE');
```

### Spatial Query:
```sql
SELECT 
  location_name,
  flood_risk_score,
  ST_DISTANCE(geom_point, ST_POINT(2.3522, 48.8566)) / 1000.0 as distance_from_paris_km
FROM demo_hc.risk_analytics.gold_flood_risk_scores
WHERE ST_DWITHIN(geom_point, ST_POINT(2.3522, 48.8566), 100000.0)
ORDER BY distance_from_paris_km;
```

---

## Verification

Run this query to verify the catalog exists:

```sql
SHOW CATALOGS LIKE 'demo_hc';
```

List all schemas:

```sql
SHOW SCHEMAS IN demo_hc;
```

Count tables in each schema:

```sql
-- Raw data tables
SELECT COUNT(*) as table_count FROM (SHOW TABLES IN demo_hc.raw_data);

-- Processed data tables
SELECT COUNT(*) as table_count FROM (SHOW TABLES IN demo_hc.processed_data);

-- Risk analytics tables
SELECT COUNT(*) as table_count FROM (SHOW TABLES IN demo_hc.risk_analytics);
```

---

## Status

✅ **All catalog name changes completed successfully**  
✅ **No remaining references to `european_risk_catalog`**  
✅ **All SQL queries updated**  
✅ **All Python code updated**  
✅ **All documentation updated**

---

## Next Steps

1. Review the updated `unity_catalog_setup.sql` file
2. Run the Unity Catalog setup script
3. Deploy or update pipelines
4. Verify all tables are created under `demo_hc` catalog
5. Update any external references (dashboards, notebooks, etc.)

---

**Change Verified:** 2025-11-12  
**Files Updated:** 11  
**Lines Changed:** ~50+ occurrences  
**Status:** ✅ Complete

