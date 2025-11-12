# Databricks notebook source
# MAGIC %md
# MAGIC # Drought Risk Transformation Pipeline
# MAGIC 
# MAGIC This notebook combines weather, terrain, and soil data to calculate
# MAGIC drought risk scores for European locations using meteorological and agricultural drought indices.
# MAGIC 
# MAGIC **Risk Factors:**
# MAGIC - Precipitation deficits (7-day, 30-day, 90-day)
# MAGIC - Temperature anomalies and heat stress
# MAGIC - Soil moisture content and water retention capacity
# MAGIC - Evapotranspiration rates
# MAGIC - Vegetation health indicators
# MAGIC - Topographic wetness index

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.databricks.sql import functions as dbf  # Databricks ST functions
from datetime import datetime, timedelta
import yaml
import math

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Load configuration
try:
    with open("/Workspace/Shared/risk_app/config/european_data_sources.yaml", 'r') as f:
        config = yaml.safe_load(f)
        DROUGHT_THRESHOLDS = config['risk_parameters']['drought_risk']['thresholds']
except:
    # Fallback thresholds
    DROUGHT_THRESHOLDS = {
        "severe_drought_spi": -2.0,
        "moderate_drought_spi": -1.5,
        "consecutive_dry_days": 30,
        "soil_moisture_critical": 0.2
    }

# Drought classification thresholds
SPI_THRESHOLDS = {
    "extremely_wet": 2.0,
    "very_wet": 1.5,
    "moderately_wet": 1.0,
    "normal": -1.0,
    "moderate_drought": -1.5,
    "severe_drought": -2.0,
    "extreme_drought": -2.5
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Combined Features for Drought Analysis

# COMMAND ----------

@dlt.table(
    name="silver_drought_terrain_weather_combined",
    comment="Silver layer: Combined terrain, soil, and weather features for drought analysis",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_drought_terrain_weather_combined():
    """
    Combine terrain, soil properties, and weather data for drought analysis.
    """
    # Read terrain features with soil data
    terrain = dlt.read("gold_terrain_features_drought_risk")
    
    # Read weather features
    weather = dlt.read("gold_weather_features_drought_risk")
    
    # Join on H3 cell
    combined = (
        terrain
        .join(
            weather,
            on="h3_cell_8",
            how="left"
        )
        .select(
            terrain.h3_cell_8.alias("h3_cell"),
            terrain.h3_cell_6.alias("h3_cell_regional"),
            F.coalesce(weather.latitude, terrain.latitude).alias("latitude"),
            F.coalesce(weather.longitude, terrain.longitude).alias("longitude"),
            F.coalesce(weather.location_name, F.lit("Unknown")).alias("location_name"),
            F.coalesce(weather.country_code, F.lit("EU")).alias("country_code"),
            # Terrain and soil features
            terrain.elevation_m,
            terrain.slope_degrees,
            terrain.topographic_wetness_index,
            terrain.soil_water_content,
            terrain.water_retention_capacity,
            terrain.drought_vulnerability_score.alias("terrain_drought_vulnerability"),
            # Weather features
            F.coalesce(weather.observation_time, F.current_timestamp()).alias("observation_time"),
            F.coalesce(weather.temperature_celsius, F.lit(15.0)).alias("temperature_celsius"),
            F.coalesce(weather.humidity_percent, F.lit(70)).alias("humidity_percent"),
            F.coalesce(weather.precipitation_summary_24h, F.lit(0.0)).alias("precipitation_24h"),
            F.coalesce(weather.dewpoint_celsius, F.lit(10.0)).alias("dewpoint_celsius"),
            F.coalesce(weather.consecutive_dry_days_estimate, F.lit(0)).alias("dry_days_estimate"),
            F.coalesce(weather.evapotranspiration_potential_mm, F.lit(3.0)).alias("evapotranspiration_mm"),
            F.coalesce(weather.water_deficit_daily, F.lit(0.0)).alias("water_deficit_daily"),
            F.coalesce(weather.drought_stress_indicator, F.lit("none")).alias("weather_drought_indicator"),
            # ST geospatial columns
            F.coalesce(weather.geom_point, terrain.geom_point).alias("geom_point"),
            F.coalesce(weather.geog_point, terrain.geog_point).alias("geog_point"),
            # Calculate spatial distance for quality check
            F.when(
                weather.geom_point.isNotNull() & terrain.geom_point.isNotNull(),
                dbf.st_distance(terrain.geom_point, weather.geom_point)
            ).otherwise(0.0).alias("geom_match_distance_m"),
            F.current_timestamp().alias("processing_timestamp")
        )
        # Create drought impact zones
        .withColumn("drought_impact_zone_10km", dbf.st_buffer(F.col("geom_point"), 10000.0))
        .withColumn("drought_impact_zone_25km", dbf.st_buffer(F.col("geom_point"), 25000.0))
        # Calculate area covered by drought monitoring
        .withColumn("monitoring_area_10km_km2", 
            dbf.st_area(F.col("drought_impact_zone_10km")) / 1000000.0
        )
        .withColumn("monitoring_area_25km_km2", 
            dbf.st_area(F.col("drought_impact_zone_25km")) / 1000000.0
        )
    )
    
    return combined


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Historical Precipitation Analysis

# COMMAND ----------

@dlt.table(
    name="silver_drought_precipitation_history",
    comment="Silver layer: Historical precipitation analysis for drought indices",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_drought_precipitation_history():
    """
    Calculate rolling precipitation metrics for drought analysis.
    This would ideally use historical data; here we create a simplified version.
    """
    combined = dlt.read("silver_drought_terrain_weather_combined")
    
    # Calculate rolling precipitation windows
    window_spec_7d = Window.partitionBy("h3_cell").orderBy("observation_time").rowsBetween(-6, 0)
    window_spec_30d = Window.partitionBy("h3_cell").orderBy("observation_time").rowsBetween(-29, 0)
    window_spec_90d = Window.partitionBy("h3_cell").orderBy("observation_time").rowsBetween(-89, 0)
    
    precip_history = (
        combined
        .withColumn("observation_date", F.to_date("observation_time"))
        # Rolling sums (in real implementation, this would aggregate daily data)
        .withColumn("precipitation_7d", 
            F.sum("precipitation_24h").over(window_spec_7d)
        )
        .withColumn("precipitation_30d",
            F.sum("precipitation_24h").over(window_spec_30d)
        )
        .withColumn("precipitation_90d",
            F.sum("precipitation_24h").over(window_spec_90d)
        )
        # Calculate normals (simplified - would use historical climatology)
        .withColumn("precipitation_7d_normal", F.lit(35.0))   # mm
        .withColumn("precipitation_30d_normal", F.lit(150.0)) # mm
        .withColumn("precipitation_90d_normal", F.lit(450.0)) # mm
        .withColumn("temperature_normal", F.lit(12.0))        # Celsius
        # Calculate anomalies
        .withColumn("precipitation_7d_anomaly",
            F.col("precipitation_7d") - F.col("precipitation_7d_normal")
        )
        .withColumn("precipitation_30d_anomaly",
            F.col("precipitation_30d") - F.col("precipitation_30d_normal")
        )
        .withColumn("precipitation_90d_anomaly",
            F.col("precipitation_90d") - F.col("precipitation_90d_normal")
        )
        .withColumn("temperature_anomaly",
            F.col("temperature_celsius") - F.col("temperature_normal")
        )
        # Calculate anomaly percentages
        .withColumn("precipitation_7d_anomaly_pct",
            (F.col("precipitation_7d_anomaly") / F.col("precipitation_7d_normal") * 100)
        )
        .withColumn("precipitation_30d_anomaly_pct",
            (F.col("precipitation_30d_anomaly") / F.col("precipitation_30d_normal") * 100)
        )
        .withColumn("precipitation_90d_anomaly_pct",
            (F.col("precipitation_90d_anomaly") / F.col("precipitation_90d_normal") * 100)
        )
    )
    
    return precip_history


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Drought Indices

# COMMAND ----------

@dlt.table(
    name="gold_drought_indices",
    comment="Gold layer: Standardized drought indices (SPI, SPEI, SMI)",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_drought_indices():
    """
    Calculate standardized drought indices:
    - SPI: Standardized Precipitation Index
    - SPEI: Standardized Precipitation Evapotranspiration Index
    - SMI: Soil Moisture Index
    - VCI: Vegetation Condition Index (placeholder)
    """
    precip_history = dlt.read("silver_drought_precipitation_history")
    
    drought_indices = (
        precip_history
        # Simplified SPI calculation (normally requires gamma distribution fitting)
        # SPI = (X - mean) / std_dev
        .withColumn("spi_7d",
            F.col("precipitation_7d_anomaly") / F.lit(15.0)  # Simplified std dev
        )
        .withColumn("spi_30d",
            F.col("precipitation_30d_anomaly") / F.lit(50.0)
        )
        .withColumn("spi_90d",
            F.col("precipitation_90d_anomaly") / F.lit(100.0)
        )
        # SPEI: SPI adjusted for evapotranspiration
        .withColumn("water_balance_7d",
            F.col("precipitation_7d") - (F.col("evapotranspiration_mm") * 7)
        )
        .withColumn("water_balance_30d",
            F.col("precipitation_30d") - (F.col("evapotranspiration_mm") * 30)
        )
        .withColumn("spei_30d",
            (F.col("water_balance_30d") - F.lit(100.0)) / F.lit(60.0)  # Simplified
        )
        # Soil Moisture Index (0-1 scale)
        .withColumn("smi",
            F.greatest(
                F.least(
                    F.col("soil_water_content") - (F.col("water_deficit_daily") * 0.01),
                    F.lit(1.0)
                ),
                F.lit(0.0)
            )
        )
        # Vegetation Condition Index placeholder (would come from satellite data)
        .withColumn("vci",
            F.when(F.col("temperature_anomaly") > 5, 0.3)
            .when(F.col("temperature_anomaly") > 2, 0.5)
            .when(F.col("precipitation_30d_anomaly") < -50, 0.4)
            .otherwise(0.7)
        )
        # Combined Drought Indicator
        .withColumn("combined_drought_indicator",
            (F.col("spi_30d") * 0.3 +
             F.col("spei_30d") * 0.3 +
             (F.col("smi") - 0.5) * 2 * 0.2 +  # Scale SMI to -1 to 1
             (F.col("vci") - 0.5) * 2 * 0.2)
        )
    )
    
    return drought_indices


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Drought Risk Scores

# COMMAND ----------

@dlt.table(
    name="gold_drought_risk_scores",
    comment="Gold layer: Comprehensive drought risk scores with contributing factors",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_drought_risk_scores():
    """
    Calculate comprehensive drought risk scores.
    
    Risk Score Components:
    1. Meteorological Drought (30%): Precipitation deficits
    2. Agricultural Drought (40%): Soil moisture and vegetation stress
    3. Hydrological Conditions (20%): Water balance and evapotranspiration
    4. Terrain Vulnerability (10%): Slope, TWI, water retention capacity
    """
    drought_indices = dlt.read("gold_drought_indices")
    
    drought_risk = (
        drought_indices
        # --- Meteorological Drought Risk (0-100) ---
        .withColumn("meteorological_drought_risk",
            F.when(F.col("spi_30d") <= -2.0, 100.0)
            .when(F.col("spi_30d") <= -1.5, 80.0)
            .when(F.col("spi_30d") <= -1.0, 60.0)
            .when(F.col("spi_30d") <= -0.5, 40.0)
            .when(F.col("spi_30d") < 0, 20.0)
            .otherwise(5.0)
        )
        # --- Agricultural Drought Risk (0-100) ---
        .withColumn("soil_moisture_risk",
            F.when(F.col("smi") <= 0.2, 100.0)
            .when(F.col("smi") <= 0.3, 80.0)
            .when(F.col("smi") <= 0.4, 60.0)
            .when(F.col("smi") <= 0.5, 40.0)
            .when(F.col("smi") <= 0.6, 20.0)
            .otherwise(5.0)
        )
        .withColumn("vegetation_stress_risk",
            F.when(F.col("vci") <= 0.2, 100.0)
            .when(F.col("vci") <= 0.3, 80.0)
            .when(F.col("vci") <= 0.4, 60.0)
            .when(F.col("vci") <= 0.5, 40.0)
            .when(F.col("vci") <= 0.6, 20.0)
            .otherwise(5.0)
        )
        .withColumn("agricultural_drought_risk",
            (F.col("soil_moisture_risk") * 0.6 + F.col("vegetation_stress_risk") * 0.4)
        )
        # --- Hydrological Drought Risk (0-100) ---
        .withColumn("water_balance_risk",
            F.when(F.col("water_deficit_daily") >= 10, 90.0)
            .when(F.col("water_deficit_daily") >= 7, 70.0)
            .when(F.col("water_deficit_daily") >= 5, 50.0)
            .when(F.col("water_deficit_daily") >= 3, 30.0)
            .when(F.col("water_deficit_daily") > 0, 15.0)
            .otherwise(5.0)
        )
        .withColumn("evapotranspiration_risk",
            F.when(F.col("evapotranspiration_mm") >= 7, 80.0)
            .when(F.col("evapotranspiration_mm") >= 5, 60.0)
            .when(F.col("evapotranspiration_mm") >= 3, 40.0)
            .otherwise(20.0)
        )
        .withColumn("hydrological_drought_risk",
            (F.col("water_balance_risk") * 0.7 + F.col("evapotranspiration_risk") * 0.3)
        )
        # --- Terrain Vulnerability (0-100) ---
        .withColumn("terrain_vulnerability_risk",
            F.col("terrain_drought_vulnerability") * 100.0
        )
        # --- Combined Drought Risk Score (0-100) ---
        .withColumn("drought_risk_score",
            (F.col("meteorological_drought_risk") * 0.30 +
             F.col("agricultural_drought_risk") * 0.40 +
             F.col("hydrological_drought_risk") * 0.20 +
             F.col("terrain_vulnerability_risk") * 0.10)
        )
        # --- Risk Classification ---
        .withColumn("drought_risk_category",
            F.when(F.col("drought_risk_score") >= 80, "EXTREME")
            .when(F.col("drought_risk_score") >= 60, "SEVERE")
            .when(F.col("drought_risk_score") >= 40, "MODERATE")
            .when(F.col("drought_risk_score") >= 20, "ABNORMALLY_DRY")
            .otherwise("NORMAL")
        )
        # --- Drought Type Classification ---
        .withColumn("drought_type",
            F.when(
                (F.col("meteorological_drought_risk") >= 60) &
                (F.col("agricultural_drought_risk") >= 60),
                "COMPREHENSIVE"
            ).when(
                F.col("meteorological_drought_risk") >= 60,
                "METEOROLOGICAL"
            ).when(
                F.col("agricultural_drought_risk") >= 60,
                "AGRICULTURAL"
            ).when(
                F.col("hydrological_drought_risk") >= 60,
                "HYDROLOGICAL"
            ).otherwise("EMERGING")
        )
        # --- Confidence Score ---
        .withColumn("confidence_score",
            F.when(F.col("precipitation_90d") > 0, 0.90)
            .when(F.col("precipitation_30d") > 0, 0.80)
            .otherwise(0.65)
        )
        # --- Alert Flags ---
        .withColumn("critical_alert",
            F.when(
                (F.col("drought_risk_score") >= 80) |
                ((F.col("spi_30d") <= -2.0) & (F.col("smi") <= 0.3)),
                True
            ).otherwise(False)
        )
        .withColumn("monitoring_required",
            F.when(
                (F.col("drought_risk_score") >= 60) |
                (F.col("spi_30d") <= -1.5) |
                (F.col("smi") <= 0.4),
                True
            ).otherwise(False)
        )
        # --- Duration Estimate ---
        .withColumn("estimated_duration_days",
            F.when(F.col("drought_risk_score") >= 80, F.lit(60))
            .when(F.col("drought_risk_score") >= 60, F.lit(45))
            .when(F.col("drought_risk_score") >= 40, F.lit(30))
            .otherwise(F.lit(14))
        )
        # ST geospatial: Create water restriction zones based on drought severity
        .withColumn("restriction_zone_radius_m",
            F.when(F.col("drought_risk_category") == "EXTREME", 25000.0)
            .when(F.col("drought_risk_category") == "SEVERE", 15000.0)
            .when(F.col("drought_risk_category") == "MODERATE", 10000.0)
            .otherwise(5000.0)
        )
        .withColumn("restriction_zone", 
            dbf.st_buffer(F.col("geom_point"), F.col("restriction_zone_radius_m"))
        )
        # Calculate affected area
        .withColumn("affected_area_km2", 
            dbf.st_area(F.col("restriction_zone")) / 1000000.0
        )
        # Export geometries for GIS integration
        .withColumn("location_geojson", dbf.st_asgeojson(F.col("geom_point")))
        .withColumn("restriction_zone_geojson", dbf.st_asgeojson(F.col("restriction_zone")))
        # Create WKT for external systems
        .withColumn("location_wkt", dbf.st_aswkt(F.col("geom_point")))
        .withColumn("processing_timestamp", F.current_timestamp())
    )
    
    return drought_risk


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Drought Risk Regional Summary

# COMMAND ----------

@dlt.table(
    name="gold_drought_risk_regional_summary",
    comment="Gold layer: Regional drought risk aggregations for dashboard and reporting",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_drought_risk_regional_summary():
    """
    Aggregate drought risk scores by region (H3 level 6) for broader analysis.
    """
    drought_scores = dlt.read("gold_drought_risk_scores")
    
    regional_summary = (
        drought_scores
        .groupBy(
            F.col("h3_cell_regional"),
            F.col("country_code")
        )
        .agg(
            F.count("*").alias("total_cells"),
            F.avg("drought_risk_score").alias("avg_drought_risk_score"),
            F.max("drought_risk_score").alias("max_drought_risk_score"),
            F.min("drought_risk_score").alias("min_drought_risk_score"),
            F.avg("meteorological_drought_risk").alias("avg_meteorological_risk"),
            F.avg("agricultural_drought_risk").alias("avg_agricultural_risk"),
            F.avg("hydrological_drought_risk").alias("avg_hydrological_risk"),
            F.avg("spi_30d").alias("avg_spi_30d"),
            F.avg("smi").alias("avg_soil_moisture_index"),
            F.sum(F.when(F.col("drought_risk_category") == "EXTREME", 1).otherwise(0)).alias("extreme_cells"),
            F.sum(F.when(F.col("drought_risk_category") == "SEVERE", 1).otherwise(0)).alias("severe_cells"),
            F.sum(F.when(F.col("drought_risk_category") == "MODERATE", 1).otherwise(0)).alias("moderate_cells"),
            F.sum(F.when(F.col("critical_alert"), 1).otherwise(0)).alias("critical_alert_count"),
            F.sum(F.when(F.col("monitoring_required"), 1).otherwise(0)).alias("monitoring_required_count"),
            F.avg("precipitation_30d").alias("avg_precipitation_30d"),
            F.avg("temperature_celsius").alias("avg_temperature"),
            F.avg("temperature_anomaly").alias("avg_temperature_anomaly"),
            F.avg("confidence_score").alias("avg_confidence_score"),
            F.max("observation_time").alias("latest_observation"),
            F.current_timestamp().alias("processing_timestamp")
        )
        .withColumn("regional_risk_category",
            F.when(F.col("avg_drought_risk_score") >= 80, "EXTREME")
            .when(F.col("avg_drought_risk_score") >= 60, "SEVERE")
            .when(F.col("avg_drought_risk_score") >= 40, "MODERATE")
            .when(F.col("avg_drought_risk_score") >= 20, "ABNORMALLY_DRY")
            .otherwise("NORMAL")
        )
        .withColumn("alert_percentage",
            (F.col("critical_alert_count") / F.col("total_cells") * 100).cast("decimal(5,2)")
        )
        .withColumn("affected_area_pct",
            ((F.col("extreme_cells") + F.col("severe_cells") + F.col("moderate_cells")) / 
             F.col("total_cells") * 100).cast("decimal(5,2)")
        )
    )
    
    return regional_summary


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Drought Risk Time Series

# COMMAND ----------

@dlt.table(
    name="gold_drought_risk_time_series",
    comment="Gold layer: Time series of drought conditions for trend analysis",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_drought_risk_time_series():
    """
    Create time series view of drought risk for trending and forecasting.
    """
    drought_scores = dlt.read("gold_drought_risk_scores")
    
    time_series = (
        drought_scores
        .withColumn("observation_date", F.to_date("observation_time"))
        .groupBy(
            F.col("h3_cell"),
            F.col("location_name"),
            F.col("country_code"),
            F.col("observation_date")
        )
        .agg(
            F.avg("drought_risk_score").alias("avg_drought_risk_score"),
            F.max("drought_risk_score").alias("max_drought_risk_score"),
            F.avg("spi_30d").alias("avg_spi_30d"),
            F.avg("smi").alias("avg_soil_moisture_index"),
            F.sum("precipitation_24h").alias("total_precipitation"),
            F.avg("temperature_celsius").alias("avg_temperature"),
            F.avg("evapotranspiration_mm").alias("avg_evapotranspiration"),
            F.max("critical_alert").alias("had_critical_alert"),
            F.first("latitude").alias("latitude"),
            F.first("longitude").alias("longitude"),
            F.current_timestamp().alias("processing_timestamp")
        )
        # Calculate 7-day rolling average
        .withColumn("drought_risk_7d_avg",
            F.avg("avg_drought_risk_score").over(
                Window
                .partitionBy("h3_cell")
                .orderBy("observation_date")
                .rowsBetween(-6, 0)
            )
        )
        .withColumn("spi_7d_avg",
            F.avg("avg_spi_30d").over(
                Window
                .partitionBy("h3_cell")
                .orderBy("observation_date")
                .rowsBetween(-6, 0)
            )
        )
        # Calculate trend
        .withColumn("risk_trend",
            F.when(F.col("drought_risk_7d_avg") - F.lag("drought_risk_7d_avg", 7).over(
                Window.partitionBy("h3_cell").orderBy("observation_date")
            ) > 10, "worsening")
            .when(F.col("drought_risk_7d_avg") - F.lag("drought_risk_7d_avg", 7).over(
                Window.partitionBy("h3_cell").orderBy("observation_date")
            ) < -10, "improving")
            .otherwise("stable")
        )
    )
    
    return time_series


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Drought High Risk Alerts

# COMMAND ----------

@dlt.table(
    name="gold_drought_high_risk_alerts",
    comment="Gold layer: Active high-risk drought locations requiring immediate attention",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_drought_high_risk_alerts():
    """
    Filter and enrich high-risk drought locations for alerting systems.
    """
    drought_scores = dlt.read("gold_drought_risk_scores")
    
    alerts = (
        drought_scores
        .filter(
            (F.col("drought_risk_category").isin(["EXTREME", "SEVERE"])) |
            (F.col("critical_alert") == True)
        )
        .select(
            F.col("h3_cell"),
            F.col("h3_cell_regional"),
            F.col("location_name"),
            F.col("country_code"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("drought_risk_score"),
            F.col("drought_risk_category"),
            F.col("drought_type"),
            F.col("meteorological_drought_risk"),
            F.col("agricultural_drought_risk"),
            F.col("hydrological_drought_risk"),
            F.col("spi_30d"),
            F.col("smi"),
            F.col("vci"),
            F.col("precipitation_30d"),
            F.col("precipitation_30d_anomaly_pct"),
            F.col("temperature_anomaly"),
            F.col("water_deficit_daily"),
            F.col("estimated_duration_days"),
            F.col("critical_alert"),
            F.col("monitoring_required"),
            F.col("confidence_score"),
            F.col("observation_time"),
            F.current_timestamp().alias("alert_timestamp")
        )
        # Add priority ranking
        .withColumn("alert_priority",
            F.when(F.col("critical_alert"), 1)
            .when(F.col("drought_risk_score") >= 80, 2)
            .when(F.col("drought_risk_score") >= 60, 3)
            .otherwise(4)
        )
        # Add recommended actions
        .withColumn("recommended_action",
            F.when(F.col("drought_risk_score") >= 80, 
                "CRITICAL: Implement water restrictions, emergency irrigation support")
            .when(F.col("drought_risk_score") >= 60,
                "ALERT: Water conservation measures, monitor crop stress")
            .when(F.col("drought_risk_score") >= 40,
                "WATCH: Voluntary water conservation, prepare contingency plans")
            .otherwise("MONITOR: Continue observation, update forecasts")
        )
        .withColumn("impact_sectors",
            F.when(F.col("agricultural_drought_risk") >= 60, 
                F.array(F.lit("agriculture"), F.lit("water_supply"))
            ).when(F.col("meteorological_drought_risk") >= 60,
                F.array(F.lit("water_supply"), F.lit("ecosystems"))
            ).otherwise(
                F.array(F.lit("monitoring"))
            )
        )
        # Add alert ID
        .withColumn("alert_id",
            F.concat(
                F.col("h3_cell"),
                F.lit("_DROUGHT_"),
                F.date_format(F.col("observation_time"), "yyyyMMdd")
            )
        )
        # Sort by priority
        .orderBy(F.col("alert_priority"), F.col("drought_risk_score").desc())
    )
    
    return alerts


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Monitoring

# COMMAND ----------

@dlt.table(
    name="gold_drought_risk_quality_metrics",
    comment="Gold layer: Data quality metrics for drought risk calculations",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_drought_risk_quality_metrics():
    """
    Monitor data quality and completeness for drought risk pipeline.
    """
    drought_scores = dlt.read("gold_drought_risk_scores")
    
    quality_metrics = (
        drought_scores
        .groupBy("country_code")
        .agg(
            F.count("*").alias("total_locations"),
            F.avg("confidence_score").alias("avg_confidence"),
            F.countDistinct("h3_cell").alias("unique_cells"),
            F.sum(F.when(F.col("precipitation_30d") > 0, 1).otherwise(0)).alias("precip_data_available"),
            F.sum(F.when(F.col("smi").isNotNull(), 1).otherwise(0)).alias("soil_moisture_available"),
            F.sum(F.when(F.col("topographic_wetness_index").isNotNull(), 1).otherwise(0)).alias("terrain_data_available"),
            F.min("observation_time").alias("earliest_observation"),
            F.max("observation_time").alias("latest_observation"),
            F.current_timestamp().alias("processing_timestamp")
        )
        .withColumn("precip_coverage_pct",
            (F.col("precip_data_available") / F.col("total_locations") * 100).cast("decimal(5,2)")
        )
        .withColumn("soil_coverage_pct",
            (F.col("soil_moisture_available") / F.col("total_locations") * 100).cast("decimal(5,2)")
        )
        .withColumn("terrain_coverage_pct",
            (F.col("terrain_data_available") / F.col("total_locations") * 100).cast("decimal(5,2)")
        )
        .withColumn("data_quality_status",
            F.when(
                (F.col("precip_coverage_pct") >= 90) & 
                (F.col("soil_coverage_pct") >= 75) &
                (F.col("avg_confidence") >= 0.85),
                "EXCELLENT"
            ).when(
                (F.col("precip_coverage_pct") >= 75) & 
                (F.col("avg_confidence") >= 0.70),
                "GOOD"
            ).when(
                (F.col("precip_coverage_pct") >= 50) & 
                (F.col("avg_confidence") >= 0.50),
                "ACCEPTABLE"
            ).otherwise("POOR")
        )
    )
    
    return quality_metrics


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration
# MAGIC 
# MAGIC **Execution Mode:** Scheduled
# MAGIC **Schedule:** Daily (drought develops slowly, daily updates sufficient)
# MAGIC **Target:** Unity Catalog - `demo_hc.risk_analytics`
# MAGIC 
# MAGIC **Dependencies:**
# MAGIC - `gold_terrain_features_drought_risk`
# MAGIC - `gold_weather_features_drought_risk`
# MAGIC 
# MAGIC **Outputs:**
# MAGIC - Drought indices (SPI, SPEI, SMI, VCI)
# MAGIC - Drought risk scores (H3 cell level)
# MAGIC - Regional summaries
# MAGIC - Time series for trending
# MAGIC - High-risk alerts
# MAGIC - Quality metrics
# MAGIC 
# MAGIC **Alert Thresholds:**
# MAGIC - EXTREME: Risk score >= 80 OR (SPI-30 <= -2.0 AND SMI <= 0.3)
# MAGIC - SEVERE: Risk score >= 60 OR SPI-30 <= -1.5
# MAGIC - Critical Alert: Risk >= 80 or severe conditions for 30+ days

