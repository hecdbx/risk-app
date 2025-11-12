-- ============================================================================
-- Unity Catalog Setup for European Climate Risk Pipeline
-- ============================================================================
-- Purpose: Create catalog and unified schema for the pipeline
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
-- STEP 2: Create Single Unified Schema
-- ============================================================================

-- Single Schema: All data layers (Bronze, Silver, Gold)
CREATE SCHEMA IF NOT EXISTS climate_risk
COMMENT 'Climate Risk Data - Unified schema for all layers (Bronze: raw data, Silver: processed, Gold: analytics)';

-- ============================================================================
-- STEP 3: Set Schema Properties
-- ============================================================================

ALTER SCHEMA climate_risk
SET DBPROPERTIES (
  'data_classification' = 'internal',
  'layers' = 'bronze,silver,gold',
  'retention_days' = '1825',  -- 5 years
  'description' = 'Unified schema for all climate risk data layers'
);

-- ============================================================================
-- STEP 4: Use the Schema
-- ============================================================================

USE SCHEMA climate_risk;

-- ============================================================================
-- STEP 5: Create Monitoring Tables
-- ============================================================================

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
  ingestion_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Log of pipeline executions for monitoring and auditing';

-- Data quality tracking
CREATE TABLE IF NOT EXISTS data_quality_tracking (
  check_id STRING,
  table_name STRING,
  check_type STRING,
  check_result STRING,
  records_passed BIGINT,
  records_failed BIGINT,
  check_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Data quality check results over time';

-- ============================================================================
-- STEP 6: Set Tags for Data Discovery
-- ============================================================================

-- Tag catalog
ALTER CATALOG demo_hc 
SET TAGS ('domain' = 'climate_risk', 'project' = 'flood_drought_assessment');

-- Tag schema
ALTER SCHEMA climate_risk 
SET TAGS ('layers' = 'bronze,silver,gold', 'business_critical' = 'true');

-- ============================================================================
-- STEP 7: Grant Permissions (Optional - adjust as needed)
-- ============================================================================

-- Uncomment and adjust these based on your user groups

-- GRANT USE CATALOG ON CATALOG demo_hc TO `data_engineers`;
-- GRANT ALL PRIVILEGES ON CATALOG demo_hc TO `data_engineers`;

-- GRANT USE CATALOG ON CATALOG demo_hc TO `data_analysts`;
-- GRANT SELECT ON SCHEMA demo_hc.climate_risk TO `data_analysts`;

-- GRANT USE CATALOG ON CATALOG demo_hc TO `risk_analysts`;
-- GRANT SELECT ON SCHEMA demo_hc.climate_risk TO `risk_analysts`;

-- ============================================================================
-- STEP 8: Verification Queries
-- ============================================================================

-- Show catalog structure
SHOW CATALOGS;
SHOW SCHEMAS IN demo_hc;

-- Show tables in climate_risk schema
SHOW TABLES IN demo_hc.climate_risk;

-- Show grants (uncomment if permissions are set)
-- SHOW GRANTS ON CATALOG demo_hc;
-- SHOW GRANTS ON SCHEMA demo_hc.climate_risk;

-- ============================================================================
-- STEP 9: Set as Default
-- ============================================================================

USE CATALOG demo_hc;
USE SCHEMA climate_risk;

-- ============================================================================
-- STEP 10: Confirmation
-- ============================================================================

SELECT 'Unity Catalog setup completed successfully!' as status;
SELECT 'Catalog: demo_hc' as info;
SELECT 'Schema: climate_risk' as schema_info;
SELECT 'Ready for pipeline deployment' as next_step;
