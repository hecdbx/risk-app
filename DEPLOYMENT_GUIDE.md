# European Climate Risk Pipeline - Deployment Guide

This guide provides step-by-step instructions for deploying the European Climate Risk Pipeline to your Databricks environment.

---

## 📋 Pre-Deployment Checklist

Before starting deployment, ensure you have:

- [ ] Databricks workspace with Unity Catalog enabled
- [ ] Databricks Runtime 14.3 LTS or higher
- [ ] Appropriate cloud storage configured (DBFS, S3, ADLS, or GCS)
- [ ] AccuWeather API key (for weather data ingestion)
- [ ] Databricks CLI installed and configured
- [ ] Required permissions:
  - Create clusters
  - Create pipelines
  - Create jobs/workflows
  - Manage Unity Catalog
  - Access to secrets

---

## 🚀 Deployment Steps

### Step 1: Prepare Your Environment

#### 1.1 Install Databricks CLI

```bash
pip install databricks-cli
databricks configure
```

Provide your Databricks workspace URL and access token when prompted.

#### 1.2 Clone the Repository

```bash
git clone <repository-url>
cd risk_app
```

---

### Step 2: Configure Secrets

#### 2.1 Create Secret Scope

```bash
# Create secret scope for AccuWeather credentials
databricks secrets create-scope accuweather

# Add your AccuWeather API key
databricks secrets put-secret accuweather api_key
# Enter your API key when prompted
```

#### 2.2 Verify Secrets

```bash
databricks secrets list-secrets accuweather
```

---

### Step 3: Upload Files to Databricks Workspace

#### 3.1 Create Workspace Directory

```bash
databricks workspace mkdirs /Workspace/Shared/risk_app
```

#### 3.2 Upload All Files

```bash
# Upload configuration
databricks workspace import \
  config/european_data_sources.yaml \
  /Workspace/Shared/risk_app/config/european_data_sources.yaml

# Upload pipelines
for pipeline in pipelines/*.py; do
  filename=$(basename "$pipeline")
  databricks workspace import \
    "$pipeline" \
    "/Workspace/Shared/risk_app/pipelines/$filename"
done

# Upload orchestrator
databricks workspace import \
  workflow_orchestrator.py \
  /Workspace/Shared/risk_app/workflow_orchestrator.py

# Upload requirements
databricks workspace import \
  requirements.txt \
  /Workspace/Shared/risk_app/requirements.txt
```

---

### Step 4: Prepare Data Storage

#### 4.1 Create Storage Directories

**For DBFS:**
```python
# Run in Databricks notebook
dbutils.fs.mkdirs("/mnt/european-climate-risk/raw/")
dbutils.fs.mkdirs("/mnt/european-climate-risk/processed/")
dbutils.fs.mkdirs("/mnt/european-climate-risk/terrain/")
dbutils.fs.mkdirs("/mnt/european-climate-risk/weather/")
```

#### 4.2 Mount External Storage (Optional)

**For Azure Blob Storage:**
```python
dbutils.fs.mount(
  source = "wasbs://climate-data@<storage-account>.blob.core.windows.net",
  mount_point = "/mnt/european-climate-risk/raw/",
  extra_configs = {
    "fs.azure.account.key.<storage-account>.blob.core.windows.net": "<access-key>"
  }
)
```

**For AWS S3:**
```python
dbutils.fs.mount(
  source = "s3a://<bucket-name>/climate-data/",
  mount_point = "/mnt/european-climate-risk/raw/",
  extra_configs = {
    "fs.s3a.access.key": "<access-key>",
    "fs.s3a.secret.key": "<secret-key>"
  }
)
```

---

### Step 5: Create Unity Catalog Schemas

#### 5.1 Run Unity Catalog Setup

Create a notebook in Databricks and run:

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS demo_hc
COMMENT 'European Climate Risk Data - Flood and Drought Analysis';

-- Create single unified schema
CREATE SCHEMA IF NOT EXISTS demo_hc.climate_risk
COMMENT 'Climate Risk Data - Unified schema for all layers (Bronze, Silver, Gold)';

-- Set as default
USE CATALOG demo_hc;
USE SCHEMA climate_risk;

-- Grant permissions (adjust as needed)
GRANT USE CATALOG ON CATALOG demo_hc TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA demo_hc.climate_risk TO `data_engineers`;

-- Grant read access to analysts
GRANT USE CATALOG ON CATALOG demo_hc TO `analysts`;
GRANT SELECT ON SCHEMA demo_hc.climate_risk TO `analysts`;
```

---

### Step 6: Create Cluster Configuration

#### 6.1 Create Cluster for Pipeline Execution

**Via UI:**
1. Go to Compute → Create Cluster
2. Configure:
   - **Cluster Name:** `european-risk-pipeline-cluster`
   - **Cluster Mode:** Standard
   - **Databricks Runtime:** 14.3 LTS or higher
   - **Node Type:** i3.2xlarge (or equivalent)
   - **Min Workers:** 2
   - **Max Workers:** 8 (with autoscaling)
   - **Enable Photon:** Yes

**Via API:**
```bash
cat > cluster-config.json << 'EOF'
{
  "cluster_name": "european-risk-pipeline-cluster",
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "i3.2xlarge",
  "autoscale": {
    "min_workers": 2,
    "max_workers": 8
  },
  "spark_conf": {
    "spark.databricks.delta.preview.enabled": "true",
    "spark.databricks.h3.enabled": "true"
  },
  "custom_tags": {
    "project": "european_climate_risk",
    "environment": "production"
  }
}
EOF

databricks clusters create --json-file cluster-config.json
```

#### 6.2 Install Libraries on Cluster

```bash
# Get cluster ID
CLUSTER_ID=$(databricks clusters list --output JSON | jq -r '.clusters[] | select(.cluster_name=="european-risk-pipeline-cluster") | .cluster_id')

# Install PyPI packages
databricks libraries install --cluster-id $CLUSTER_ID \
  --pypi-package rasterio \
  --pypi-package geopandas \
  --pypi-package h3 \
  --pypi-package pyyaml \
  --pypi-package xarray \
  --pypi-package netCDF4

# Restart cluster to apply libraries
databricks clusters restart --cluster-id $CLUSTER_ID
```

---

### Step 7: Deploy Pipelines

#### 7.1 Automated Deployment (Recommended)

Create a deployment notebook and run:

```python
%run /Workspace/Shared/risk_app/workflow_orchestrator.py

from workflow_orchestrator import EuropeanRiskPipelineOrchestrator

# Initialize orchestrator
orchestrator = EuropeanRiskPipelineOrchestrator()

# Deploy all pipelines
deployment_result = orchestrator.deploy_all_pipelines()

# Display results
displayHTML(f"""
<h2>Deployment Complete!</h2>
<ul>
  <li><strong>Catalog:</strong> {deployment_result['catalog']}</li>
  <li><strong>Pipelines Created:</strong> {len(deployment_result['pipeline_ids'])}</li>
  <li><strong>Workflow Job ID:</strong> {deployment_result['job_id']}</li>
</ul>
""")
```

#### 7.2 Manual Pipeline Creation (Alternative)

If automated deployment fails, create pipelines manually via Databricks UI:

**For each pipeline:**
1. Go to **Delta Live Tables** → **Create Pipeline**
2. Configure:
   - **Name:** See pipeline name in config
   - **Product Edition:** Advanced
   - **Notebook Libraries:** Select corresponding pipeline notebook
   - **Target:** `demo_hc.processed_data`
   - **Storage Location:** Default or custom path
   - **Cluster Mode:** Fixed size (2-4 workers)
   - **Channel:** Current

---

### Step 8: Configure Workflow Orchestration

#### 8.1 Create Workflow via UI

1. Go to **Workflows** → **Create Job**
2. Name: `european_climate_risk_workflow`
3. Add Tasks:

   **Task 1: Terrain Ingestion**
   - Type: Delta Live Tables Pipeline
   - Pipeline: `european_terrain_dem_ingestion`
   - Schedule: Weekly (Sundays at midnight)

   **Task 2: Weather Ingestion**
   - Type: Delta Live Tables Pipeline
   - Pipeline: `accuweather_europe_ingestion`
   - Depends On: Terrain Ingestion
   - Schedule: Hourly

   **Task 3: Flood Risk Transformation**
   - Type: Delta Live Tables Pipeline
   - Pipeline: `flood_risk_transformation`
   - Depends On: Terrain Ingestion, Weather Ingestion
   - Schedule: Hourly (15 minutes after weather)

   **Task 4: Drought Risk Transformation**
   - Type: Delta Live Tables Pipeline
   - Pipeline: `drought_risk_transformation`
   - Depends On: Terrain Ingestion, Weather Ingestion
   - Schedule: Daily at 6 AM UTC

4. Configure **Email Notifications** for failures

---

### Step 9: Initial Data Load

#### 9.1 Upload Terrain Data (if available)

Upload your terrain TIFF files to the storage location:

```bash
# Example for DBFS
databricks fs cp copernicus_dem/*.tif \
  dbfs:/mnt/european-climate-risk/raw/copernicus_dem/
```

#### 9.2 Run Initial Pipeline Execution

```bash
# Get pipeline IDs
TERRAIN_PIPELINE_ID="<from deployment output>"
WEATHER_PIPELINE_ID="<from deployment output>"

# Trigger terrain ingestion
databricks pipelines run-now --pipeline-id $TERRAIN_PIPELINE_ID

# Wait for completion, then trigger weather ingestion
databricks pipelines run-now --pipeline-id $WEATHER_PIPELINE_ID

# Trigger risk calculations
databricks pipelines run-now --pipeline-id $FLOOD_PIPELINE_ID
databricks pipelines run-now --pipeline-id $DROUGHT_PIPELINE_ID
```

---

### Step 10: Validation and Testing

#### 10.1 Verify Data Quality

Run validation queries:

```sql
-- Check terrain data coverage
SELECT 
  source,
  COUNT(*) as cell_count,
  COUNT(DISTINCT h3_cell_8) as unique_cells,
  AVG(elevation_m) as avg_elevation,
  AVG(data_quality_score) as avg_quality
FROM demo_hc.processed_data.silver_terrain_unified
GROUP BY source;

-- Check weather data freshness
SELECT 
  country_code,
  MAX(observation_time) as latest_observation,
  COUNT(*) as observation_count
FROM demo_hc.processed_data.silver_weather_europe_enriched
GROUP BY country_code;

-- Check flood risk scores
SELECT 
  flood_risk_category,
  COUNT(*) as count,
  AVG(flood_risk_score) as avg_score
FROM demo_hc.risk_analytics.gold_flood_risk_scores
GROUP BY flood_risk_category
ORDER BY avg_score DESC;

-- Check drought risk scores
SELECT 
  drought_risk_category,
  COUNT(*) as count,
  AVG(drought_risk_score) as avg_score
FROM demo_hc.risk_analytics.gold_drought_risk_scores
GROUP BY drought_risk_category
ORDER BY avg_score DESC;
```

#### 10.2 Review Data Quality Metrics

```sql
-- Flood risk quality metrics
SELECT * FROM demo_hc.risk_analytics.gold_flood_risk_quality_metrics;

-- Drought risk quality metrics
SELECT * FROM demo_hc.risk_analytics.gold_drought_risk_quality_metrics;
```

---

### Step 11: Set Up Monitoring and Alerts

#### 11.1 Create Alert Queries

```sql
-- High flood risk alert
CREATE OR REPLACE VIEW demo_hc.risk_analytics.flood_alerts_active AS
SELECT 
  location_name,
  country_code,
  flood_risk_score,
  flood_risk_category,
  recommended_action,
  alert_timestamp
FROM demo_hc.risk_analytics.gold_flood_high_risk_alerts
WHERE immediate_alert = true
  AND alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR;

-- High drought risk alert
CREATE OR REPLACE VIEW demo_hc.risk_analytics.drought_alerts_active AS
SELECT 
  location_name,
  country_code,
  drought_risk_score,
  drought_risk_category,
  drought_type,
  recommended_action,
  alert_timestamp
FROM demo_hc.risk_analytics.gold_drought_high_risk_alerts
WHERE critical_alert = true;
```

#### 11.2 Configure Databricks SQL Alerts

1. Go to **Databricks SQL** → **Alerts**
2. Create alerts based on the views above
3. Configure email/Slack notifications

---

### Step 12: Documentation and Handoff

#### 12.1 Create Operational Documentation

Document the following:
- Pipeline schedules and dependencies
- Data refresh frequencies
- Monitoring procedures
- Troubleshooting guide
- Contact information for support

#### 12.2 Train Users

Provide training on:
- Accessing risk scores via SQL
- Interpreting risk categories
- Understanding alert priorities
- Dashboard usage (if applicable)

---

## 🎯 Post-Deployment Tasks

### Week 1: Monitoring and Tuning
- [ ] Monitor pipeline execution times
- [ ] Verify data quality metrics
- [ ] Check for any errors or warnings
- [ ] Tune cluster sizes if needed

### Week 2: Performance Optimization
- [ ] Analyze query performance
- [ ] Optimize table partitioning
- [ ] Add Z-ordering on frequently filtered columns
- [ ] Review and adjust autoscaling settings

### Week 3: Enhancement
- [ ] Add additional European cities (if needed)
- [ ] Incorporate additional data sources
- [ ] Create dashboards and visualizations
- [ ] Set up automated reporting

---

## 🔄 Maintenance Schedule

| Task | Frequency | Owner |
|------|-----------|-------|
| Monitor pipeline health | Daily | Data Engineer |
| Review data quality metrics | Weekly | Data Analyst |
| Update AccuWeather API key | Annually | DevOps |
| Refresh terrain data | Quarterly | Data Engineer |
| Review and optimize costs | Monthly | Engineering Manager |
| Update documentation | As needed | Team |

---

## 📞 Support

For deployment issues or questions:
- **Email:** climate-risk-team@company.com
- **Slack:** #climate-risk-pipeline
- **Documentation:** [Internal Wiki]

---

## ✅ Deployment Checklist Summary

- [ ] Databricks environment prepared
- [ ] Secrets configured (AccuWeather API key)
- [ ] Files uploaded to workspace
- [ ] Storage mounted and configured
- [ ] Unity Catalog schemas created
- [ ] Cluster created and libraries installed
- [ ] Pipelines deployed (all 4)
- [ ] Workflow orchestration configured
- [ ] Initial data loaded
- [ ] Validation queries executed successfully
- [ ] Monitoring and alerts set up
- [ ] Documentation completed
- [ ] Users trained

---

**Deployment Time Estimate:** 2-4 hours (depending on data volume and experience)

**Next Steps:** Monitor the first few pipeline runs and adjust configurations as needed.

---

Good luck with your deployment! 🚀

