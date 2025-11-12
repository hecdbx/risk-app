-- ============================================================================
-- Unity Catalog Setup for European Climate Risk Pipeline
-- ============================================================================
-- Purpose: Create catalogs, schemas, and set permissions for the pipeline
-- Author: Climate Risk Analytics Team
-- Date: 2025-11-12
-- ============================================================================

-- ============================================================================
-- STEP 1: Create Main Catalog
-- ============================================================================

CREATE CATALOG IF NOT EXISTS demo_hc
COMMENT 'European Climate Risk Data - Flood and Drought Risk Assessment for Insurance Applications';

-- Use the catalog
USE CATALOG demo_hc;

-- ============================================================================
-- STEP 2: Create Schemas (Bronze, Silver, Gold Layers)
-- ============================================================================

-- Bronze Layer: Raw Data
CREATE SCHEMA IF NOT EXISTS demo_hc.raw_data
COMMENT 'Bronze Layer: Raw ingested data from external sources (EEA, Copernicus, AccuWeather)';

-- Silver Layer: Processed Data
CREATE SCHEMA IF NOT EXISTS demo_hc.processed_data
COMMENT 'Silver Layer: Cleansed, enriched, and harmonized data with quality checks';

-- Gold Layer: Analytics
CREATE SCHEMA IF NOT EXISTS demo_hc.risk_analytics
COMMENT 'Gold Layer: Risk scores, alerts, and analytics-ready datasets';

-- ============================================================================
-- STEP 3: Set Schema Properties
-- ============================================================================

ALTER SCHEMA demo_hc.raw_data 
SET DBPROPERTIES (
  'data_classification' = 'internal',
  'layer' = 'bronze',
  'retention_days' = '365'
);

ALTER SCHEMA demo_hc.processed_data 
SET DBPROPERTIES (
  'data_classification' = 'internal',
  'layer' = 'silver',
  'retention_days' = '730'
);

ALTER SCHEMA demo_hc.risk_analytics 
SET DBPROPERTIES (
  'data_classification' = 'restricted',
  'layer' = 'gold',
  'retention_days' = '1825'  -- 5 years
);

-- ============================================================================
-- STEP 4: Create Service Principal / User Groups (if not exists)
-- ============================================================================

-- Note: These commands may need to be run by a Databricks account admin

-- CREATE GROUP IF NOT EXISTS data_engineers;
-- CREATE GROUP IF NOT EXISTS data_analysts;
-- CREATE GROUP IF NOT EXISTS risk_analysts;
-- CREATE GROUP IF NOT EXISTS business_users;

-- ============================================================================
-- STEP 5: Grant Catalog Permissions
-- ============================================================================

-- Data Engineers: Full access to all layers
GRANT USE CATALOG ON CATALOG demo_hc TO `data_engineers`;
GRANT ALL PRIVILEGES ON CATALOG demo_hc TO `data_engineers`;

-- Data Analysts: Read access to processed data and analytics
GRANT USE CATALOG ON CATALOG demo_hc TO `data_analysts`;

-- Risk Analysts: Read access to analytics layer
GRANT USE CATALOG ON CATALOG demo_hc TO `risk_analysts`;

-- Business Users: Read-only access to gold layer
GRANT USE CATALOG ON CATALOG demo_hc TO `business_users`;

-- ============================================================================
-- STEP 6: Grant Schema-Level Permissions
-- ============================================================================

-- Raw Data Schema (Bronze)
GRANT ALL PRIVILEGES ON SCHEMA demo_hc.raw_data TO `data_engineers`;
GRANT SELECT ON SCHEMA demo_hc.raw_data TO `data_analysts`;

-- Processed Data Schema (Silver)
GRANT ALL PRIVILEGES ON SCHEMA demo_hc.processed_data TO `data_engineers`;
GRANT SELECT ON SCHEMA demo_hc.processed_data TO `data_analysts`;
GRANT SELECT ON SCHEMA demo_hc.processed_data TO `risk_analysts`;

-- Risk Analytics Schema (Gold)
GRANT ALL PRIVILEGES ON SCHEMA demo_hc.risk_analytics TO `data_engineers`;
GRANT SELECT ON SCHEMA demo_hc.risk_analytics TO `data_analysts`;
GRANT SELECT ON SCHEMA demo_hc.risk_analytics TO `risk_analysts`;
GRANT SELECT ON SCHEMA demo_hc.risk_analytics TO `business_users`;

-- ============================================================================
-- STEP 7: Create External Locations (for external data sources)
-- ============================================================================

-- For Azure Blob Storage
-- CREATE EXTERNAL LOCATION IF NOT EXISTS european_terrain_data
--   URL 's3://european-climate-data/terrain/'
--   WITH (STORAGE CREDENTIAL `aws_credentials`)
--   COMMENT 'External location for European terrain data (Copernicus, EEA)';

-- For AWS S3
-- CREATE EXTERNAL LOCATION IF NOT EXISTS european_terrain_data
--   URL 's3://european-climate-data/terrain/'
--   WITH (STORAGE CREDENTIAL `aws_credentials`)
--   COMMENT 'External location for European terrain data (Copernicus, EEA)';

-- ============================================================================
-- STEP 8: Create Metadata Tables
-- ============================================================================

USE SCHEMA demo_hc.risk_analytics;

-- Pipeline execution log
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
  execution_id STRING,
  pipeline_name STRING,
  execution_start_time TIMESTAMP,
  execution_end_time TIMESTAMP,
  status STRING,
  records_processed BIGINT,
  error_message STRING,
  execution_user STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Log of pipeline executions for monitoring and auditing';

-- Data quality metrics tracking
CREATE TABLE IF NOT EXISTS data_quality_tracking (
  check_id STRING,
  table_name STRING,
  check_type STRING,
  check_result STRING,
  records_passed BIGINT,
  records_failed BIGINT,
  check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Data quality check results over time';

-- ============================================================================
-- STEP 9: Create Views for Common Queries
-- ============================================================================

-- Latest flood risk by country
CREATE OR REPLACE VIEW demo_hc.risk_analytics.latest_flood_risk_by_country AS
SELECT 
  country_code,
  COUNT(*) as locations,
  AVG(flood_risk_score) as avg_flood_risk,
  SUM(CASE WHEN flood_risk_category = 'CRITICAL' THEN 1 ELSE 0 END) as critical_locations,
  SUM(CASE WHEN flood_risk_category = 'HIGH' THEN 1 ELSE 0 END) as high_risk_locations,
  SUM(CASE WHEN immediate_alert = true THEN 1 ELSE 0 END) as active_alerts,
  MAX(observation_time) as latest_update
FROM demo_hc.risk_analytics.gold_flood_risk_scores
WHERE observation_time >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY country_code
ORDER BY avg_flood_risk DESC;

-- Latest drought risk by country
CREATE OR REPLACE VIEW demo_hc.risk_analytics.latest_drought_risk_by_country AS
SELECT 
  country_code,
  COUNT(*) as locations,
  AVG(drought_risk_score) as avg_drought_risk,
  SUM(CASE WHEN drought_risk_category = 'EXTREME' THEN 1 ELSE 0 END) as extreme_locations,
  SUM(CASE WHEN drought_risk_category = 'SEVERE' THEN 1 ELSE 0 END) as severe_locations,
  SUM(CASE WHEN critical_alert = true THEN 1 ELSE 0 END) as active_alerts,
  AVG(spi_30d) as avg_spi_30d,
  MAX(observation_time) as latest_update
FROM demo_hc.risk_analytics.gold_drought_risk_scores
WHERE observation_time >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY country_code
ORDER BY avg_drought_risk DESC;

-- Combined risk dashboard
CREATE OR REPLACE VIEW demo_hc.risk_analytics.combined_risk_dashboard AS
SELECT 
  f.country_code,
  f.location_name,
  f.latitude,
  f.longitude,
  f.flood_risk_score,
  f.flood_risk_category,
  d.drought_risk_score,
  d.drought_risk_category,
  (f.flood_risk_score + d.drought_risk_score) / 2 as combined_risk_score,
  f.immediate_alert as flood_alert,
  d.critical_alert as drought_alert,
  GREATEST(f.observation_time, d.observation_time) as latest_update
FROM demo_hc.risk_analytics.gold_flood_risk_scores f
FULL OUTER JOIN demo_hc.risk_analytics.gold_drought_risk_scores d
  ON f.h3_cell = d.h3_cell
  AND f.country_code = d.country_code
WHERE f.observation_time >= current_timestamp() - INTERVAL 24 HOURS
  OR d.observation_time >= current_timestamp() - INTERVAL 7 DAYS;

-- Active alerts requiring attention
CREATE OR REPLACE VIEW demo_hc.risk_analytics.active_alerts_all AS
SELECT 
  'FLOOD' as alert_type,
  location_name,
  country_code,
  flood_risk_score as risk_score,
  flood_risk_category as risk_category,
  recommended_action,
  alert_timestamp,
  alert_priority
FROM demo_hc.risk_analytics.gold_flood_high_risk_alerts
WHERE immediate_alert = true

UNION ALL

SELECT 
  'DROUGHT' as alert_type,
  location_name,
  country_code,
  drought_risk_score as risk_score,
  drought_risk_category as risk_category,
  recommended_action,
  alert_timestamp,
  alert_priority
FROM demo_hc.risk_analytics.gold_drought_high_risk_alerts
WHERE critical_alert = true

ORDER BY alert_priority, risk_score DESC;

-- ============================================================================
-- STEP 10: Create Sample Materialized Views (for performance)
-- ============================================================================

-- Note: Materialized views require Databricks SQL or Delta Live Tables

-- Daily flood risk summary (refresh daily)
-- CREATE MATERIALIZED VIEW IF NOT EXISTS demo_hc.risk_analytics.daily_flood_summary
-- AS
-- SELECT 
--   DATE(observation_time) as date,
--   country_code,
--   AVG(flood_risk_score) as avg_risk,
--   MAX(flood_risk_score) as max_risk,
--   COUNT(DISTINCT h3_cell) as cells_monitored
-- FROM demo_hc.risk_analytics.gold_flood_risk_scores
-- GROUP BY DATE(observation_time), country_code;

-- ============================================================================
-- STEP 11: Set Row and Column Level Security (Optional)
-- ============================================================================

-- Example: Restrict access to specific countries by user group
-- ALTER TABLE demo_hc.risk_analytics.gold_flood_risk_scores
-- SET ROW FILTER country_filter ON (country_code) FOR 
--   WHEN IS_MEMBER('france_team') THEN country_code = 'FR'
--   WHEN IS_MEMBER('germany_team') THEN country_code = 'DE'
--   ELSE false;

-- ============================================================================
-- STEP 12: Create Tags for Data Discovery
-- ============================================================================

-- Tag catalog
ALTER CATALOG demo_hc 
SET TAGS ('domain' = 'climate_risk', 'project' = 'flood_drought_assessment');

-- Tag schemas
ALTER SCHEMA demo_hc.raw_data 
SET TAGS ('layer' = 'bronze', 'pii' = 'false');

ALTER SCHEMA demo_hc.processed_data 
SET TAGS ('layer' = 'silver', 'pii' = 'false');

ALTER SCHEMA demo_hc.risk_analytics 
SET TAGS ('layer' = 'gold', 'pii' = 'false', 'business_critical' = 'true');

-- ============================================================================
-- STEP 13: Verification Queries
-- ============================================================================

-- Show catalog structure
SHOW CATALOGS;
SHOW SCHEMAS IN demo_hc;

-- Show tables in each schema
SHOW TABLES IN demo_hc.raw_data;
SHOW TABLES IN demo_hc.processed_data;
SHOW TABLES IN demo_hc.risk_analytics;

-- Show grants
SHOW GRANTS ON CATALOG demo_hc;
SHOW GRANTS ON SCHEMA demo_hc.raw_data;
SHOW GRANTS ON SCHEMA demo_hc.processed_data;
SHOW GRANTS ON SCHEMA demo_hc.risk_analytics;

-- ============================================================================
-- STEP 14: Set Catalog as Default
-- ============================================================================

USE CATALOG demo_hc;
USE SCHEMA risk_analytics;

-- ============================================================================
-- Setup Complete!
-- ============================================================================

SELECT 'Unity Catalog setup completed successfully!' as status;
SELECT 'Catalog: demo_hc' as info;
SELECT 'Ready for pipeline deployment' as next_step;

