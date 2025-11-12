# Databricks notebook source
# MAGIC %md
# MAGIC # Climate Risk Transformation Pipeline (Combined: Flood + Drought)
# MAGIC 
# MAGIC This notebook combines terrain, weather, and hydrological data to calculate
# MAGIC both flood and drought risk scores for European locations using advanced geospatial analytics.
# MAGIC 
# MAGIC ## Part 1: Flood Risk Analysis
# MAGIC **Risk Factors:**
# MAGIC - Terrain elevation and slope
# MAGIC - Flow accumulation and drainage patterns
# MAGIC - Recent and forecast precipitation
# MAGIC - Soil saturation levels
# MAGIC - Proximity to water bodies
# MAGIC - Historical flood events
# MAGIC
# MAGIC ## Part 2: Drought Risk Analysis
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
        FLOOD_THRESHOLDS = config['risk_parameters']['flood_risk']['thresholds']
        DROUGHT_THRESHOLDS = config['risk_parameters']['drought_risk']['thresholds']
except:
    # Fallback thresholds
    FLOOD_THRESHOLDS = {
        "extreme_precipitation_daily": 100,
        "high_precipitation_daily": 50,
        "slope_flood_prone": 5,
        "twi_flood_threshold": 15
    }
    DROUGHT_THRESHOLDS = {
        "severe_drought_spi": -2.0,
        "moderate_drought_spi": -1.5,
        "consecutive_dry_days": 30,
        "soil_moisture_critical": 0.2
    }

# H3 resolution for analysis
H3_RESOLUTION = 8  # City/neighborhood level

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
# MAGIC # ============================================================================
# MAGIC # PART 1: FLOOD RISK TRANSFORMATION
# MAGIC # ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flood Risk - Silver Layer: Combined Terrain-Weather Features

# COMMAND ----------

@dlt.table(
    name="silver_flood_terrain_weather_combined",
    comment="Silver layer: Combined terrain and weather features for flood analysis",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_flood_terrain_weather_combined():
    """
    Combine terrain features with current weather observations for flood analysis.
    """
    # Read terrain features
    terrain = dlt.read("gold_terrain_features_flood_risk")
    
    # Read weather features
    weather = dlt.read("gold_weather_features_flood_risk")
    
    # Join on H3 cell - use spatial join with ST functions
    combined = (
        terrain
        .join(
            weather,
            on="h3_cell_8",
            how="left"
        )
        # Add spatial validation using ST_DWithin (within 30km)
        .withColumn("spatial_match_valid",
            F.when(
                weather.geom_point.isNotNull() & terrain.geom_point.isNotNull(),
                dbf.st_dwithin(terrain.geom_point, weather.geom_point, 30000.0)
            ).otherwise(True)
        )
        .filter(F.col("spatial_match_valid"))
        .select(
            terrain.h3_cell_8.alias("h3_cell"),
            terrain.h3_cell_6.alias("h3_cell_regional"),
            F.coalesce(weather.latitude, terrain.latitude).alias("latitude"),
            F.coalesce(weather.longitude, terrain.longitude).alias("longitude"),
            F.coalesce(weather.location_name, F.lit("Unknown")).alias("location_name"),
            F.coalesce(weather.country_code, F.lit("EU")).alias("country_code"),
            # Terrain features
            terrain.elevation_m,
            terrain.slope_degrees,
            terrain.terrain_roughness,
            terrain.elevation_range_m,
            terrain.low_elevation_flag,
            terrain.gentle_slope_flag,
            terrain.flood_accumulation_potential,
            # Weather features
            F.coalesce(weather.observation_time, F.current_timestamp()).alias("observation_time"),
            F.coalesce(weather.temperature_celsius, F.lit(15.0)).alias("temperature_celsius"),
            F.coalesce(weather.humidity_percent, F.lit(70)).alias("humidity_percent"),
            F.coalesce(weather.precipitation_summary_1h, F.lit(0.0)).alias("precipitation_1h"),
            F.coalesce(weather.precipitation_summary_6h, F.lit(0.0)).alias("precipitation_6h"),
            F.coalesce(weather.precipitation_summary_12h, F.lit(0.0)).alias("precipitation_12h"),
            F.coalesce(weather.precipitation_summary_24h, F.lit(0.0)).alias("precipitation_24h"),
            F.coalesce(weather.precipitation_24h_class, F.lit("none")).alias("precipitation_class"),
            F.coalesce(weather.flood_risk_indicator, F.lit("none")).alias("weather_flood_indicator"),
            F.coalesce(weather.soil_saturation_proxy, F.lit(0.3)).alias("soil_saturation"),
            # ST geospatial columns
            F.coalesce(weather.geom_point, terrain.geom_point).alias("geom_point"),
            F.coalesce(weather.geog_point, terrain.geog_point).alias("geog_point"),
            # Calculate distance for quality
            F.when(
                weather.geom_point.isNotNull() & terrain.geom_point.isNotNull(),
                dbf.st_distance(terrain.geom_point, weather.geom_point)
            ).otherwise(0.0).alias("geom_match_distance_m"),
            F.current_timestamp().alias("processing_timestamp")
        )
        # Create impact zones
        .withColumn("flood_impact_zone_1km", dbf.st_buffer(F.col("geom_point"), 1000.0))
        .withColumn("impact_zone_1km_area_m2", dbf.st_area(F.col("flood_impact_zone_1km")))
    )
    
    return combined


# COMMAND ----------

# MAGIC %md
# MAGIC ## Flood Risk - Gold Layer: Flood Risk Scores

# COMMAND ----------

@dlt.table(
    name="gold_flood_risk_scores",
    comment="Gold layer: Comprehensive flood risk scores with contributing factors",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_flood_risk_scores():
    """
    Calculate comprehensive flood risk scores.
    """
    combined = dlt.read("silver_flood_terrain_weather_combined")
    
    flood_risk = (
        combined
        # Terrain Risk Component
        .withColumn("terrain_risk_elevation",
            F.when(F.col("elevation_m") < 50, 90.0)
            .when(F.col("elevation_m") < 100, 70.0)
            .when(F.col("elevation_m") < 200, 40.0)
            .when(F.col("elevation_m") < 500, 20.0)
            .otherwise(5.0)
        )
        .withColumn("terrain_risk_slope",
            F.when(F.col("slope_degrees") < 2, 90.0)
            .when(F.col("slope_degrees") < 5, 70.0)
            .when(F.col("slope_degrees") < 10, 40.0)
            .when(F.col("slope_degrees") < 20, 20.0)
            .otherwise(5.0)
        )
        .withColumn("terrain_risk_score",
            (F.col("terrain_risk_elevation") * 0.6 + F.col("terrain_risk_slope") * 0.4)
        )
        # Weather Risk Component
        .withColumn("weather_risk_intensity",
            F.when(F.col("precipitation_1h") >= 20, 100.0)
            .when(F.col("precipitation_1h") >= 10, 80.0)
            .when(F.col("precipitation_1h") >= 5, 50.0)
            .when(F.col("precipitation_1h") >= 2, 30.0)
            .otherwise(10.0)
        )
        .withColumn("weather_risk_accumulation",
            F.when(F.col("precipitation_24h") >= 100, 100.0)
            .when(F.col("precipitation_24h") >= 50, 80.0)
            .when(F.col("precipitation_24h") >= 25, 50.0)
            .when(F.col("precipitation_24h") >= 10, 30.0)
            .otherwise(5.0)
        )
        .withColumn("weather_risk_score",
            F.greatest(
                (F.col("weather_risk_intensity") * 0.6 + F.col("weather_risk_accumulation") * 0.4),
                F.when(F.col("weather_flood_indicator") == "high", F.lit(85.0))
                .when(F.col("weather_flood_indicator") == "moderate", F.lit(60.0))
                .otherwise(F.lit(10.0))
            )
        )
        # Saturation Risk
        .withColumn("saturation_risk_score", F.col("soil_saturation") * 100.0)
        # Combined Flood Risk Score
        .withColumn("flood_risk_score",
            (F.col("terrain_risk_score") * 0.40 +
             F.col("weather_risk_score") * 0.40 +
             F.col("saturation_risk_score") * 0.20)
        )
        # Risk Classification
        .withColumn("flood_risk_category",
            F.when(F.col("flood_risk_score") >= 80, "CRITICAL")
            .when(F.col("flood_risk_score") >= 60, "HIGH")
            .when(F.col("flood_risk_score") >= 40, "MODERATE")
            .when(F.col("flood_risk_score") >= 20, "LOW")
            .otherwise("MINIMAL")
        )
        .withColumn("confidence_score",
            F.when(F.col("weather_flood_indicator") != "none", 0.95)
            .when(F.col("precipitation_24h") > 0, 0.85)
            .otherwise(0.70)
        )
        # Alert Flags
        .withColumn("immediate_alert",
            F.when(
                (F.col("flood_risk_score") >= 80) |
                ((F.col("precipitation_1h") >= 20) & (F.col("elevation_m") < 100)),
                True
            ).otherwise(False)
        )
        .withColumn("monitoring_required",
            F.when((F.col("flood_risk_score") >= 60) | (F.col("precipitation_24h") >= 50), True)
            .otherwise(False)
        )
        # ST geospatial: evacuation zones
        .withColumn("evacuation_zone_radius_m",
            F.when(F.col("flood_risk_category") == "CRITICAL", 5000.0)
            .when(F.col("flood_risk_category") == "HIGH", 3000.0)
            .when(F.col("flood_risk_category") == "MODERATE", 1000.0)
            .otherwise(500.0)
        )
        .withColumn("evacuation_zone", dbf.st_buffer(F.col("geom_point"), F.col("evacuation_zone_radius_m")))
        .withColumn("evacuation_zone_area_km2", dbf.st_area(F.col("evacuation_zone")) / 1000000.0)
        .withColumn("location_geojson", dbf.st_asgeojson(F.col("geom_point")))
        .withColumn("evacuation_zone_geojson", dbf.st_asgeojson(F.col("evacuation_zone")))
        .withColumn("processing_timestamp", F.current_timestamp())
    )
    
    return flood_risk


# COMMAND ----------

# MAGIC %md
# MAGIC ## Flood Risk - Gold Layer: Regional Summary

# COMMAND ----------

@dlt.table(
    name="gold_flood_risk_regional_summary",
    comment="Gold layer: Regional flood risk aggregations",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_flood_risk_regional_summary():
    """Aggregate flood risk by region."""
    flood_scores = dlt.read("gold_flood_risk_scores")
    
    return (
        flood_scores
        .groupBy(F.col("h3_cell_regional"), F.col("country_code"))
        .agg(
            F.count("*").alias("total_cells"),
            F.avg("flood_risk_score").alias("avg_flood_risk_score"),
            F.max("flood_risk_score").alias("max_flood_risk_score"),
            F.avg("terrain_risk_score").alias("avg_terrain_risk"),
            F.avg("weather_risk_score").alias("avg_weather_risk"),
            F.sum(F.when(F.col("flood_risk_category") == "CRITICAL", 1).otherwise(0)).alias("critical_cells"),
            F.sum(F.when(F.col("immediate_alert"), 1).otherwise(0)).alias("immediate_alert_count"),
            F.current_timestamp().alias("processing_timestamp")
        )
        .withColumn("regional_risk_category",
            F.when(F.col("avg_flood_risk_score") >= 80, "CRITICAL")
            .when(F.col("avg_flood_risk_score") >= 60, "HIGH")
            .when(F.col("avg_flood_risk_score") >= 40, "MODERATE")
            .otherwise("LOW")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC # ============================================================================
# MAGIC # PART 2: DROUGHT RISK TRANSFORMATION
# MAGIC # ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drought Risk - Silver Layer: Combined Features

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
        .join(weather, on="h3_cell_8", how="left")
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
        .withColumn("monitoring_area_10km_km2", 
            dbf.st_area(F.col("drought_impact_zone_10km")) / 1000000.0
        )
    )
    
    return combined


# COMMAND ----------

# MAGIC %md
# MAGIC ## Drought Risk - Silver Layer: Precipitation History

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
    """
    combined = dlt.read("silver_drought_terrain_weather_combined")
    
    # Calculate rolling precipitation windows
    window_spec_7d = Window.partitionBy("h3_cell").orderBy("observation_time").rowsBetween(-6, 0)
    window_spec_30d = Window.partitionBy("h3_cell").orderBy("observation_time").rowsBetween(-29, 0)
    window_spec_90d = Window.partitionBy("h3_cell").orderBy("observation_time").rowsBetween(-89, 0)
    
    precip_history = (
        combined
        .withColumn("observation_date", F.to_date("observation_time"))
        # Rolling sums
        .withColumn("precipitation_7d", F.sum("precipitation_24h").over(window_spec_7d))
        .withColumn("precipitation_30d", F.sum("precipitation_24h").over(window_spec_30d))
        .withColumn("precipitation_90d", F.sum("precipitation_24h").over(window_spec_90d))
        # Calculate normals (simplified)
        .withColumn("precipitation_7d_normal", F.lit(35.0))
        .withColumn("precipitation_30d_normal", F.lit(150.0))
        .withColumn("precipitation_90d_normal", F.lit(450.0))
        .withColumn("temperature_normal", F.lit(12.0))
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
        # Anomaly percentages
        .withColumn("precipitation_30d_anomaly_pct",
            (F.col("precipitation_30d_anomaly") / F.col("precipitation_30d_normal") * 100)
        )
    )
    
    return precip_history


# COMMAND ----------

# MAGIC %md
# MAGIC ## Drought Risk - Gold Layer: Drought Indices

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
    Calculate standardized drought indices.
    """
    precip_history = dlt.read("silver_drought_precipitation_history")
    
    drought_indices = (
        precip_history
        # Simplified SPI calculation
        .withColumn("spi_7d", F.col("precipitation_7d_anomaly") / F.lit(15.0))
        .withColumn("spi_30d", F.col("precipitation_30d_anomaly") / F.lit(50.0))
        .withColumn("spi_90d", F.col("precipitation_90d_anomaly") / F.lit(100.0))
        # SPEI: SPI adjusted for evapotranspiration
        .withColumn("water_balance_7d",
            F.col("precipitation_7d") - (F.col("evapotranspiration_mm") * 7)
        )
        .withColumn("water_balance_30d",
            F.col("precipitation_30d") - (F.col("evapotranspiration_mm") * 30)
        )
        .withColumn("spei_30d",
            (F.col("water_balance_30d") - F.lit(100.0)) / F.lit(60.0)
        )
        # Soil Moisture Index
        .withColumn("smi",
            F.greatest(
                F.least(
                    F.col("soil_water_content") - (F.col("water_deficit_daily") * 0.01),
                    F.lit(1.0)
                ),
                F.lit(0.0)
            )
        )
        # Vegetation Condition Index placeholder
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
             (F.col("smi") - 0.5) * 2 * 0.2 +
             (F.col("vci") - 0.5) * 2 * 0.2)
        )
    )
    
    return drought_indices


# COMMAND ----------

# MAGIC %md
# MAGIC ## Drought Risk - Gold Layer: Drought Risk Scores

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
    """
    drought_indices = dlt.read("gold_drought_indices")
    
    drought_risk = (
        drought_indices
        # Meteorological Drought Risk
        .withColumn("meteorological_drought_risk",
            F.when(F.col("spi_30d") <= -2.0, 100.0)
            .when(F.col("spi_30d") <= -1.5, 80.0)
            .when(F.col("spi_30d") <= -1.0, 60.0)
            .when(F.col("spi_30d") <= -0.5, 40.0)
            .when(F.col("spi_30d") < 0, 20.0)
            .otherwise(5.0)
        )
        # Agricultural Drought Risk
        .withColumn("soil_moisture_risk",
            F.when(F.col("smi") <= 0.2, 100.0)
            .when(F.col("smi") <= 0.3, 80.0)
            .when(F.col("smi") <= 0.4, 60.0)
            .when(F.col("smi") <= 0.5, 40.0)
            .otherwise(5.0)
        )
        .withColumn("vegetation_stress_risk",
            F.when(F.col("vci") <= 0.2, 100.0)
            .when(F.col("vci") <= 0.3, 80.0)
            .when(F.col("vci") <= 0.4, 60.0)
            .otherwise(5.0)
        )
        .withColumn("agricultural_drought_risk",
            (F.col("soil_moisture_risk") * 0.6 + F.col("vegetation_stress_risk") * 0.4)
        )
        # Hydrological Drought Risk
        .withColumn("water_balance_risk",
            F.when(F.col("water_deficit_daily") >= 10, 90.0)
            .when(F.col("water_deficit_daily") >= 7, 70.0)
            .when(F.col("water_deficit_daily") >= 5, 50.0)
            .otherwise(5.0)
        )
        .withColumn("hydrological_drought_risk",
            F.col("water_balance_risk")
        )
        # Terrain Vulnerability
        .withColumn("terrain_vulnerability_risk",
            F.col("terrain_drought_vulnerability") * 100.0
        )
        # Combined Drought Risk Score
        .withColumn("drought_risk_score",
            (F.col("meteorological_drought_risk") * 0.30 +
             F.col("agricultural_drought_risk") * 0.40 +
             F.col("hydrological_drought_risk") * 0.20 +
             F.col("terrain_vulnerability_risk") * 0.10)
        )
        # Risk Classification
        .withColumn("drought_risk_category",
            F.when(F.col("drought_risk_score") >= 80, "EXTREME")
            .when(F.col("drought_risk_score") >= 60, "SEVERE")
            .when(F.col("drought_risk_score") >= 40, "MODERATE")
            .when(F.col("drought_risk_score") >= 20, "ABNORMALLY_DRY")
            .otherwise("NORMAL")
        )
        # Drought Type
        .withColumn("drought_type",
            F.when(
                (F.col("meteorological_drought_risk") >= 60) &
                (F.col("agricultural_drought_risk") >= 60),
                "COMPREHENSIVE"
            ).when(F.col("meteorological_drought_risk") >= 60, "METEOROLOGICAL")
            .when(F.col("agricultural_drought_risk") >= 60, "AGRICULTURAL")
            .when(F.col("hydrological_drought_risk") >= 60, "HYDROLOGICAL")
            .otherwise("EMERGING")
        )
        .withColumn("confidence_score",
            F.when(F.col("precipitation_90d") > 0, 0.90)
            .when(F.col("precipitation_30d") > 0, 0.80)
            .otherwise(0.65)
        )
        # Alert Flags
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
        # ST geospatial: water restriction zones
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
        # Export geometries
        .withColumn("location_geojson", dbf.st_asgeojson(F.col("geom_point")))
        .withColumn("restriction_zone_geojson", dbf.st_asgeojson(F.col("restriction_zone")))
        .withColumn("location_wkt", dbf.st_aswkt(F.col("geom_point")))
        .withColumn("processing_timestamp", F.current_timestamp())
    )
    
    return drought_risk


# COMMAND ----------

# MAGIC %md
# MAGIC ## Drought Risk - Gold Layer: Regional Summary

# COMMAND ----------

@dlt.table(
    name="gold_drought_risk_regional_summary",
    comment="Gold layer: Regional drought risk aggregations",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_drought_risk_regional_summary():
    """Aggregate drought risk by region."""
    drought_scores = dlt.read("gold_drought_risk_scores")
    
    return (
        drought_scores
        .groupBy(F.col("h3_cell_regional"), F.col("country_code"))
        .agg(
            F.count("*").alias("total_cells"),
            F.avg("drought_risk_score").alias("avg_drought_risk_score"),
            F.max("drought_risk_score").alias("max_drought_risk_score"),
            F.avg("meteorological_drought_risk").alias("avg_meteorological_risk"),
            F.avg("agricultural_drought_risk").alias("avg_agricultural_risk"),
            F.avg("spi_30d").alias("avg_spi_30d"),
            F.avg("smi").alias("avg_soil_moisture_index"),
            F.sum(F.when(F.col("drought_risk_category") == "EXTREME", 1).otherwise(0)).alias("extreme_cells"),
            F.sum(F.when(F.col("critical_alert"), 1).otherwise(0)).alias("critical_alert_count"),
            F.current_timestamp().alias("processing_timestamp")
        )
        .withColumn("regional_risk_category",
            F.when(F.col("avg_drought_risk_score") >= 80, "EXTREME")
            .when(F.col("avg_drought_risk_score") >= 60, "SEVERE")
            .when(F.col("avg_drought_risk_score") >= 40, "MODERATE")
            .otherwise("NORMAL")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration
# MAGIC 
# MAGIC **Execution Mode:** Scheduled
# MAGIC **Schedule:** Hourly (aligned with weather data updates)
# MAGIC **Target:** Unity Catalog - `demo_hc.climate_risk`
# MAGIC 
# MAGIC **Dependencies:**
# MAGIC - `gold_terrain_features_flood_risk`
# MAGIC - `gold_terrain_features_drought_risk`
# MAGIC - `gold_weather_features_flood_risk`
# MAGIC - `gold_weather_features_drought_risk`
# MAGIC 
# MAGIC **Outputs:**
# MAGIC - Flood risk scores and summaries
# MAGIC - Drought risk scores and summaries
# MAGIC - Combined climate risk analytics

