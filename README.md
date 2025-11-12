# European Climate Risk Pipeline
## Flood and Drought Risk Assessment using Databricks

<div align="center">

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Databricks](https://img.shields.io/badge/Databricks-DBR%2014.3%2B-orange.svg)
![Status](https://img.shields.io/badge/status-production-green.svg)

</div>

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Pipeline Details](#pipeline-details)
- [Risk Scoring Methodology](#risk-scoring-methodology)
- [API Reference](#api-reference)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## 🌍 Overview

The **European Climate Risk Pipeline** is a comprehensive, production-grade data engineering solution built on Databricks that assesses flood and drought risks across Europe. It combines multiple data sources including terrain elevation data (DEM), real-time weather observations, and geospatial analysis to generate actionable risk scores.

### Key Capabilities

- **Multi-source data ingestion** from European agencies (EEA, ESA Copernicus) and weather APIs
- **Real-time risk assessment** with hourly updates for flood risk and daily updates for drought risk
- **Spatial analytics** using H3 hexagonal indexing for efficient geospatial operations
- **Delta Live Tables (DLT)** for automated data quality and lineage tracking
- **Unity Catalog integration** for governance and discoverability
- **Production-ready orchestration** with Databricks Workflows

---

## ✨ Features

### Data Ingestion
- ✅ **Terrain Data (TIFF format)**
  - Copernicus DEM (30m/90m resolution) from European Space Agency
  - EU-DEM (25m resolution) from European Environment Agency
  - OpenGeoHub soil and terrain layers (250m resolution)
  - GeoHarmonizer harmonized environmental datasets
- ✅ **Weather Data**
  - AccuWeather API for 15+ European capital cities
  - Real-time current conditions and 5-day forecasts
  - Historical precipitation data
- ✅ **Raster Data Processing**
  - Cloud-Optimized GeoTIFF (COG) support
  - Efficient tiled reading and processing
  - Automatic reprojection to standard CRS

### Risk Analytics
- 🌊 **Flood Risk Assessment**
  - Terrain-based risk (elevation, slope, flow accumulation)
  - Weather-based risk (precipitation intensity and accumulation)
  - Soil saturation modeling
  - Real-time alerts for high-risk areas
- 🌵 **Drought Risk Assessment**
  - Standardized Precipitation Index (SPI)
  - Soil Moisture Index (SMI)
  - Evapotranspiration calculations
  - Multi-timescale analysis (7-day, 30-day, 90-day)

### Spatial Analytics
- 🗺️ **H3 Hexagonal Indexing**
  - Multi-resolution spatial aggregation (levels 6, 8, 10)
  - Efficient neighbor analysis
  - Regional summaries
- 📊 **Time Series Analysis**
  - Historical trending
  - Risk score evolution
  - Anomaly detection

### Data Quality & Governance
- ✅ Automated data quality checks with DLT expectations
- ✅ Data lineage tracking
- ✅ Unity Catalog for data governance
- ✅ Comprehensive quality metrics and monitoring

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
├──────────────────┬──────────────────┬──────────────────────────┤
│  Terrain Data    │  Weather Data    │  Satellite Data (future) │
│  • Copernicus    │  • AccuWeather   │  • Sentinel-1/2          │
│  • EEA           │  • Copernicus    │  • MODIS                 │
│  • OpenGeoHub    │    Climate       │                          │
│  • GeoHarmonizer │                  │                          │
└──────────────────┴──────────────────┴──────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER (Bronze)                      │
│  • 01_terrain_dem_ingestion.py                                  │
│  • 02_accuweather_europe_ingestion.py                           │
│                                                                  │
│  Delta Live Tables with automated quality checks                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  TRANSFORMATION LAYER (Silver)                   │
│  • Unified terrain model with multi-source prioritization       │
│  • Weather data enrichment with spatial indexing                │
│  • Terrain derivatives (slope, TWI, roughness)                  │
│  • H3 spatial indexing at multiple resolutions                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ANALYTICS LAYER (Gold)                         │
│  • 03_flood_risk_transformation.py                              │
│  • 04_drought_risk_transformation.py                            │
│                                                                  │
│  Risk Scores (0-100) | Risk Categories | Alerts | Time Series   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ORCHESTRATION & SERVING                        │
│  • Databricks Workflows (workflow_orchestrator.py)              │
│  • Unity Catalog (demo_hc)                        │
│  • Delta Lake Storage (optimized, indexed)                      │
│  • REST APIs / Dashboards / Alerts                              │
└─────────────────────────────────────────────────────────────────┘
```

### Pipeline Flow

1. **Data Ingestion (Bronze Layer)**
   - Raw data from external sources
   - Format conversion (TIFF → Delta)
   - Initial quality validation

2. **Data Processing (Silver Layer)**
   - Data cleansing and enrichment
   - Spatial indexing (H3)
   - Feature engineering
   - Multi-source data fusion

3. **Risk Analytics (Gold Layer)**
   - Risk score calculation
   - Alert generation
   - Time series aggregation
   - Regional summaries

4. **Orchestration**
   - Scheduled execution (hourly/daily/weekly)
   - Dependency management
   - Error handling and retries
   - Monitoring and alerting

---

## 📦 Data Sources

### Terrain and Elevation Data

| Source | Provider | Resolution | Coverage | Format | Update Frequency |
|--------|----------|------------|----------|--------|------------------|
| **Copernicus DEM** | ESA | 30m, 90m | Global | GeoTIFF | Static |
| **EU-DEM** | EEA | 25m | EU27 + EFTA | GeoTIFF | Yearly |
| **OpenGeoHub** | OpenGeoHub Foundation | 250m | Europe | COG | Yearly |
| **GeoHarmonizer** | OpenGeoHub | 250m, 1km | Global | COG | Quarterly |

### Weather Data

| Source | Provider | Variables | Coverage | Update Frequency |
|--------|----------|-----------|----------|------------------|
| **AccuWeather** | AccuWeather Inc. | Temperature, Precipitation, Humidity, Wind | 15 European Capitals | Hourly |
| **Copernicus Climate** | ECMWF | ERA5-Land reanalysis | Europe | Daily |

### European Locations Covered

- 🇫🇷 France (Paris)
- 🇩🇪 Germany (Berlin)
- 🇪🇸 Spain (Madrid)
- 🇮🇹 Italy (Rome)
- 🇳🇱 Netherlands (Amsterdam)
- 🇧🇪 Belgium (Brussels)
- 🇦🇹 Austria (Vienna)
- 🇵🇱 Poland (Warsaw)
- 🇷🇴 Romania (Bucharest)
- 🇬🇷 Greece (Athens)
- 🇸🇪 Sweden (Stockholm)
- 🇩🇰 Denmark (Copenhagen)
- 🇫🇮 Finland (Helsinki)
- 🇵🇹 Portugal (Lisbon)
- 🇮🇪 Ireland (Dublin)

---

## 🚀 Installation

### Prerequisites

- **Databricks Workspace** (Azure, AWS, or GCP)
- **Databricks Runtime** 14.3 LTS or higher
- **Unity Catalog** enabled
- **Python** 3.10+
- **AccuWeather API Key** (optional, for weather data)

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd risk_app
```

### Step 2: Upload to Databricks Workspace

```bash
databricks workspace import-dir \
  --overwrite \
  . \
  /Workspace/Shared/risk_app
```

### Step 3: Install Dependencies

Create a Databricks cluster with the following configuration:

**Cluster Configuration:**
- **Runtime:** 14.3 LTS or higher
- **Node Type:** i3.2xlarge (or equivalent)
- **Workers:** 2-8 (depending on data volume)
- **Photon:** Enabled

**Install Libraries:**

Via cluster UI or CLI:
```bash
databricks libraries install \
  --cluster-id <cluster-id> \
  --pypi-package rasterio \
  --pypi-package geopandas \
  --pypi-package h3 \
  --pypi-package pyyaml
```

Or via notebook:
```python
%pip install -r requirements.txt
```

### Step 4: Configure Secrets

Store your AccuWeather API key in Databricks Secrets:

```bash
databricks secrets create-scope accuweather
databricks secrets put-secret accuweather api_key
```

---

## ⚙️ Configuration

### 1. Update Data Sources Configuration

Edit `config/european_data_sources.yaml`:

```yaml
# Update AccuWeather API configuration
weather_data:
  accuweather_europe:
    api_version: "v1"
    authentication: "api_key"
    # Add/remove European cities as needed
    european_capitals:
      - {key: "178087", name: "Paris, France", lat: 48.8566, lon: 2.3522, country: "FR"}
      # ... more cities
```

### 2. Configure Storage Paths

Update storage paths in the configuration:

```yaml
storage:
  raw_data: "/mnt/european-climate-risk/raw/"
  processed_data: "/mnt/european-climate-risk/processed/"
  terrain_tiles: "/mnt/european-climate-risk/terrain/"
```

### 3. Mount External Storage (if using cloud storage)

```python
# Example for Azure Blob Storage
dbutils.fs.mount(
  source = "wasbs://terrain-data@<storage-account>.blob.core.windows.net",
  mount_point = "/mnt/european-climate-risk/raw/",
  extra_configs = {"<conf-key>": "<conf-value>"}
)
```

---

## 📖 Usage

### Option 1: Automated Deployment (Recommended)

Deploy all pipelines and orchestration using the orchestrator script:

```python
from workflow_orchestrator import EuropeanRiskPipelineOrchestrator

# Initialize orchestrator
orchestrator = EuropeanRiskPipelineOrchestrator()

# Deploy all pipelines
deployment_result = orchestrator.deploy_all_pipelines()

# View deployment summary
print(deployment_result)
```

This will:
1. ✅ Create Unity Catalog schemas
2. ✅ Deploy all 4 DLT pipelines
3. ✅ Create orchestration workflow
4. ✅ Schedule automated runs

### Option 2: Manual Pipeline Execution

#### Run Terrain Ingestion

```python
# In Databricks notebook
%run ./pipelines/01_terrain_dem_ingestion
```

#### Run Weather Ingestion

```python
%run ./pipelines/02_accuweather_europe_ingestion
```

#### Run Flood Risk Analysis

```python
%run ./pipelines/03_flood_risk_transformation
```

#### Run Drought Risk Analysis

```python
%run ./pipelines/04_drought_risk_transformation
```

### Option 3: Using Databricks CLI

```bash
# Trigger a specific pipeline
databricks pipelines run-now --pipeline-id <pipeline-id>

# Monitor pipeline status
databricks pipelines get --pipeline-id <pipeline-id>
```

---

## 🔍 Pipeline Details

### Pipeline 1: Terrain/DEM Ingestion

**File:** `pipelines/01_terrain_dem_ingestion.py`

**Purpose:** Ingest and process terrain elevation data from multiple European sources

**Features:**
- Reads GeoTIFF files from multiple sources
- Prioritizes higher-resolution data (Copernicus 30m > EEA 25m > OpenGeoHub 250m)
- Calculates terrain derivatives (slope, aspect, roughness)
- H3 spatial indexing + ST GEOMETRY/GEOGRAPHY points
- ST buffer zones (250m) for neighborhood analysis
- Quality validation

**Tables Created:**
- `bronze_terrain_copernicus_dem`
- `bronze_terrain_eea_elevation`
- `bronze_terrain_opengeohub`
- `bronze_terrain_geoharmonizer`
- `silver_terrain_unified`
- `silver_terrain_derivatives`
- `gold_terrain_features_flood_risk`
- `gold_terrain_features_drought_risk`

**Schedule:** Weekly (terrain data is relatively static)

### Pipeline 2: AccuWeather Europe Ingestion

**File:** `pipelines/02_accuweather_europe_ingestion.py`

**Purpose:** Ingest real-time weather data from AccuWeather API

**Features:**
- Fetches current conditions and 5-day forecasts
- Covers 15 European capital cities
- H3 spatial indexing + ST GEOMETRY/GEOGRAPHY points
- ST buffer zones (5km, 10km, 25km) for weather impact analysis
- Data quality scoring
- Precipitation classification
- Weather severity assessment

**Tables Created:**
- `bronze_accuweather_europe_current`
- `bronze_accuweather_europe_forecast`
- `silver_weather_europe_enriched`
- `gold_weather_features_flood_risk`
- `gold_weather_features_drought_risk`
- `gold_weather_pipeline_metrics`

**Schedule:** Hourly

### Pipeline 3: Flood Risk Transformation

**File:** `pipelines/03_flood_risk_transformation.py`

**Purpose:** Calculate comprehensive flood risk scores

**Risk Components:**
1. **Terrain Risk (40%)**: Elevation, slope, drainage potential
2. **Weather Risk (40%)**: Precipitation intensity and accumulation
3. **Soil Saturation (20%)**: Current moisture levels

**ST Geospatial Features:**
- `st_dwithin` spatial validation (30km threshold)
- `st_buffer` for dynamic evacuation zones (500m - 5km)
- `st_area` for impact zone calculations
- `st_distance` for data quality verification
- GeoJSON export for web mapping

**Tables Created:**
- `silver_flood_terrain_weather_combined`
- `gold_flood_risk_scores`
- `gold_flood_risk_regional_summary`
- `gold_flood_risk_time_series`
- `gold_flood_high_risk_alerts`
- `gold_flood_risk_quality_metrics`

**Risk Categories:**
- **CRITICAL**: Score ≥ 80 (Immediate action required)
- **HIGH**: Score ≥ 60 (Emergency services on standby)
- **MODERATE**: Score ≥ 40 (Monitor conditions)
- **LOW**: Score ≥ 20 (Routine monitoring)
- **MINIMAL**: Score < 20 (Normal conditions)

**Schedule:** Hourly (aligned with weather updates)

### Pipeline 4: Drought Risk Transformation

**File:** `pipelines/04_drought_risk_transformation.py`

**Purpose:** Calculate comprehensive drought risk scores

**Drought Indices Calculated:**
- **SPI**: Standardized Precipitation Index (7-day, 30-day, 90-day)
- **SPEI**: Standardized Precipitation Evapotranspiration Index
- **SMI**: Soil Moisture Index
- **VCI**: Vegetation Condition Index (placeholder)

**Risk Components:**
1. **Meteorological Drought (30%)**: Precipitation deficits
2. **Agricultural Drought (40%)**: Soil moisture and vegetation stress
3. **Hydrological Conditions (20%)**: Water balance and ET
4. **Terrain Vulnerability (10%)**: Slope, TWI, water retention

**ST Geospatial Features:**
- `st_buffer` for water restriction zones (5km - 25km)
- `st_area` for affected area calculation
- `st_distance` for monitoring point validation
- GeoJSON and WKT export for GIS integration

**Tables Created:**
- `silver_drought_terrain_weather_combined`
- `silver_drought_precipitation_history`
- `gold_drought_indices`
- `gold_drought_risk_scores`
- `gold_drought_risk_regional_summary`
- `gold_drought_risk_time_series`
- `gold_drought_high_risk_alerts`
- `gold_drought_risk_quality_metrics`

**Risk Categories:**
- **EXTREME**: Score ≥ 80 (Critical water restrictions)
- **SEVERE**: Score ≥ 60 (Water conservation measures)
- **MODERATE**: Score ≥ 40 (Voluntary conservation)
- **ABNORMALLY_DRY**: Score ≥ 20 (Monitor conditions)
- **NORMAL**: Score < 20 (Normal conditions)

**Schedule:** Daily at 6 AM UTC

---

## 📊 Risk Scoring Methodology

### Flood Risk Calculation

```
Flood Risk Score (0-100) = 
  (Terrain Risk × 0.40) +
  (Weather Risk × 0.40) +
  (Soil Saturation × 0.20)

Where:
  Terrain Risk = (Elevation Risk × 0.6) + (Slope Risk × 0.4)
  Weather Risk = (Intensity Risk × 0.6) + (Accumulation Risk × 0.4)
  Soil Saturation = Current moisture proxy (0-1) × 100
```

**Risk Factors:**

| Factor | High Risk | Moderate Risk | Low Risk |
|--------|-----------|---------------|----------|
| **Elevation** | < 50m | 50-200m | > 200m |
| **Slope** | < 2° | 2-10° | > 10° |
| **Precipitation (1h)** | > 20mm | 10-20mm | < 10mm |
| **Precipitation (24h)** | > 100mm | 50-100mm | < 50mm |

### Drought Risk Calculation

```
Drought Risk Score (0-100) = 
  (Meteorological Drought × 0.30) +
  (Agricultural Drought × 0.40) +
  (Hydrological Conditions × 0.20) +
  (Terrain Vulnerability × 0.10)

Where:
  Agricultural Drought = (Soil Moisture Risk × 0.6) + (Vegetation Stress × 0.4)
  Hydrological Conditions = (Water Balance × 0.7) + (Evapotranspiration × 0.3)
```

**Drought Indicators:**

| Indicator | Value | Classification |
|-----------|-------|----------------|
| **SPI-30** | ≤ -2.0 | Extreme Drought |
| **SPI-30** | -1.5 to -2.0 | Severe Drought |
| **SPI-30** | -1.0 to -1.5 | Moderate Drought |
| **SMI** | ≤ 0.2 | Critical |
| **SMI** | 0.2-0.4 | Stressed |
| **SMI** | > 0.4 | Normal |

---

## 🔌 API Reference

### Query Risk Scores with Spatial Filters

```python
# Get flood risk scores with evacuation zones
flood_risk = spark.sql("""
  SELECT 
    location_name,
    flood_risk_score,
    flood_risk_category,
    evacuation_zone_area_km2,
    location_geojson,
    evacuation_zone_geojson,
    immediate_alert,
    observation_time
  FROM demo_hc.risk_analytics.gold_flood_risk_scores
  WHERE country_code = 'FR'
    AND observation_time >= current_timestamp() - INTERVAL 24 HOURS
  ORDER BY flood_risk_score DESC
""")

# Query with spatial distance filter
nearby_risks = spark.sql("""
  SELECT 
    location_name,
    flood_risk_score,
    ST_DISTANCE(geom_point, ST_POINT(2.3522, 48.8566)) / 1000.0 as distance_from_paris_km
  FROM demo_hc.risk_analytics.gold_flood_risk_scores
  WHERE ST_DWITHIN(geom_point, ST_POINT(2.3522, 48.8566), 100000.0)
  ORDER BY distance_from_paris_km
""")

# Get drought risk regional summary
drought_summary = spark.sql("""
  SELECT 
    country_code,
    avg_drought_risk_score,
    regional_risk_category,
    extreme_cells + severe_cells as high_risk_cells,
    affected_area_pct
  FROM demo_hc.risk_analytics.gold_drought_risk_regional_summary
  ORDER BY avg_drought_risk_score DESC
""")
```

### Get Active Alerts

```python
# Flood alerts
flood_alerts = spark.sql("""
  SELECT *
  FROM demo_hc.risk_analytics.gold_flood_high_risk_alerts
  WHERE immediate_alert = true
  ORDER BY alert_priority, flood_risk_score DESC
""")

# Drought alerts
drought_alerts = spark.sql("""
  SELECT *
  FROM demo_hc.risk_analytics.gold_drought_high_risk_alerts
  WHERE critical_alert = true
  ORDER BY alert_priority, drought_risk_score DESC
""")
```

---

## 🐛 Troubleshooting

### Common Issues

#### Issue 1: GDAL/Rasterio Installation Errors

**Solution:**
```bash
# Install system dependencies on cluster init script
#!/bin/bash
sudo apt-get update
sudo apt-get install -y gdal-bin libgdal-dev
pip install GDAL==$(gdal-config --version)
pip install rasterio
```

#### Issue 2: AccuWeather API Rate Limits

**Solution:** Reduce polling frequency or upgrade to higher API tier
```python
# Adjust rate limiting in configuration
API_CALLS_PER_MINUTE = 25  # Adjust based on your tier
DELAY_BETWEEN_CALLS = 60.0 / API_CALLS_PER_MINUTE
```

#### Issue 3: Out of Memory Processing Large TIFF Files

**Solution:** Use sampling or tiling
```python
# In read_geotiff_to_dataframe function
sample_factor = 0.5  # Sample every other pixel
```

#### Issue 4: H3 Function Not Found

**Solution:** Enable H3 expressions in Spark
```python
spark.conf.set("spark.databricks.h3.enabled", "true")
```

### Debug Mode

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add unit tests for new features
- Update documentation
- Ensure all pipelines pass quality checks

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 📧 Contact & Support

- **Email:** climate-risk-team@company.com
- **Issues:** [GitHub Issues](https://github.com/your-org/european-risk-pipeline/issues)
- **Documentation:** [Wiki](https://github.com/your-org/european-risk-pipeline/wiki)

---

## 🙏 Acknowledgments

- **European Space Agency (ESA)** - Copernicus DEM data
- **European Environment Agency (EEA)** - EU-DEM and water monitoring data
- **OpenGeoHub Foundation** - Terrain and soil datasets
- **AccuWeather** - Real-time weather data
- **Databricks** - Platform and Delta Lake technology

---

## 📚 References

- [Copernicus DEM Documentation](https://spacedata.copernicus.eu/collections/copernicus-digital-elevation-model)
- [EEA Data Portal](https://www.eea.europa.eu/data-and-maps)
- [H3 Spatial Indexing](https://h3geo.org/)
- [Databricks ST Geospatial Functions](https://docs.databricks.com/sql/language-manual/functions/alphabetical-list-st-geospatial-functions.html)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)
- [AccuWeather API](https://developer.accuweather.com/)
- [ST Geospatial Guide](ST_GEOSPATIAL_GUIDE.md) - Detailed usage examples

---

<div align="center">

**Built with ❤️ for Climate Resilience**

</div>

