CREATE CATALOG IF NOT EXISTS demo_hc
COMMENT 'European Climate Risk Data - Flood and Drought Risk Assessment for Insurance Applications';

USE CATALOG demo_hc;

CREATE SCHEMA IF NOT EXISTS raw_data
COMMENT 'Bronze Layer: Raw ingested data from external sources (EEA, Copernicus, AccuWeather)';

CREATE SCHEMA IF NOT EXISTS processed_data
COMMENT 'Silver Layer: Cleansed, enriched, and harmonized data with quality checks';

CREATE SCHEMA IF NOT EXISTS risk_analytics
COMMENT 'Gold Layer: Risk scores, alerts, and analytics-ready datasets';

ALTER SCHEMA raw_data 
SET DBPROPERTIES (
  'data_classification' = 'internal',
  'layer' = 'bronze',
  'retention_days' = '365'
);

ALTER SCHEMA processed_data 
SET DBPROPERTIES (
  'data_classification' = 'internal',
  'layer' = 'silver',
  'retention_days' = '730'
);

ALTER SCHEMA risk_analytics 
SET DBPROPERTIES (
  'data_classification' = 'restricted',
  'layer' = 'gold',
  'retention_days' = '1825'
);

USE SCHEMA risk_analytics;

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

ALTER CATALOG demo_hc 
SET TAGS ('domain' = 'r&d', 'project' = 'flood_drought_assessment');

ALTER SCHEMA raw_data 
SET TAGS ('layer' = 'bronze');

ALTER SCHEMA processed_data 
SET TAGS ('layer' = 'silver');

ALTER SCHEMA risk_analytics 
SET TAGS ('layer' = 'gold', 'business_critical' = 'true');

SHOW CATALOGS;
SHOW SCHEMAS IN demo_hc;

SHOW TABLES IN raw_data;
SHOW TABLES IN processed_data;
SHOW TABLES IN risk_analytics;

SHOW GRANTS ON CATALOG demo_hc;
SHOW GRANTS ON SCHEMA raw_data;
SHOW GRANTS ON SCHEMA processed_data;
SHOW GRANTS ON SCHEMA risk_analytics;

USE CATALOG demo_hc;
USE SCHEMA risk_analytics;

SELECT 'Unity Catalog setup completed successfully!' as status;
SELECT 'Catalog: demo_hc' as info;
SELECT 'Ready for pipeline deployment' as next_step;