# Databricks notebook source
# MAGIC %md
# MAGIC # Flood Risk Transformation Pipeline
# MAGIC 
# MAGIC This notebook combines terrain, weather, and hydrological data to calculate
# MAGIC flood risk scores for European locations using advanced geospatial analytics.
# MAGIC 
# MAGIC **Risk Factors:**
# MAGIC - Terrain elevation and slope
# MAGIC - Flow accumulation and drainage patterns
# MAGIC - Recent and forecast precipitation
# MAGIC - Soil saturation levels
# MAGIC - Proximity to water bodies
# MAGIC - Historical flood events

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Load configuration
try:
    with open("/Workspace/Shared/risk_app/config/european_data_sources.yaml", 'r') as f:
        config = yaml.safe_load(f)
        FLOOD_THRESHOLDS = config['risk_parameters']['flood_risk']['thresholds']
except:
    # Fallback thresholds
    FLOOD_THRESHOLDS = {
        "extreme_precipitation_daily": 100,
        "high_precipitation_daily": 50,
        "slope_flood_prone": 5,
        "twi_flood_threshold": 15
    }

# H3 resolution for analysis
H3_RESOLUTION = 8  # City/neighborhood level

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Combined Terrain-Weather Features

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
    Combine terrain features with current weather observations.
    """
    # Read terrain features
    terrain = dlt.read("gold_terrain_features_flood_risk")
    
    # Read weather features
    weather = dlt.read("gold_weather_features_flood_risk")
    
    # Join on H3 cell - use spatial join with ST functions
    # First join by H3 for efficiency, then validate with ST_DWithin
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
            ).otherwise(True)  # If no geometry, trust H3 match
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
            # Calculate distance between terrain and weather points (for quality assessment)
            F.when(
                weather.geom_point.isNotNull() & terrain.geom_point.isNotNull(),
                dbf.st_distance(terrain.geom_point, weather.geom_point)
            ).otherwise(0.0).alias("geom_match_distance_m"),
            F.current_timestamp().alias("processing_timestamp")
        )
        # Create impact zones using ST buffers
        .withColumn("flood_impact_zone_1km", dbf.st_buffer(F.col("geom_point"), 1000.0))
        .withColumn("flood_impact_zone_5km", dbf.st_buffer(F.col("geom_point"), 5000.0))
        # Calculate area of impact zones
        .withColumn("impact_zone_1km_area_m2", dbf.st_area(F.col("flood_impact_zone_1km")))
        .withColumn("impact_zone_5km_area_m2", dbf.st_area(F.col("flood_impact_zone_5km")))
    )
    
    return combined


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Flood Risk Scores

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
    
    Risk Score Components:
    1. Terrain Risk (40%): Elevation, slope, drainage
    2. Weather Risk (40%): Precipitation intensity and accumulation
    3. Soil Saturation Risk (20%): Current moisture levels
    """
    combined = dlt.read("silver_flood_terrain_weather_combined")
    
    flood_risk = (
        combined
        # --- Terrain Risk Component (0-100) ---
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
        # --- Weather Risk Component (0-100) ---
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
                .when(F.col("weather_flood_indicator") == "low", F.lit(35.0))
                .otherwise(F.lit(10.0))
            )
        )
        # --- Soil Saturation Risk Component (0-100) ---
        .withColumn("saturation_risk_score",
            F.col("soil_saturation") * 100.0
        )
        # --- Combined Flood Risk Score (0-100) ---
        .withColumn("flood_risk_score",
            (F.col("terrain_risk_score") * 0.40 +
             F.col("weather_risk_score") * 0.40 +
             F.col("saturation_risk_score") * 0.20)
        )
        # --- Risk Classification ---
        .withColumn("flood_risk_category",
            F.when(F.col("flood_risk_score") >= 80, "CRITICAL")
            .when(F.col("flood_risk_score") >= 60, "HIGH")
            .when(F.col("flood_risk_score") >= 40, "MODERATE")
            .when(F.col("flood_risk_score") >= 20, "LOW")
            .otherwise("MINIMAL")
        )
        # --- Confidence Score ---
        .withColumn("confidence_score",
            F.when(F.col("weather_flood_indicator") != "none", 0.95)
            .when(F.col("precipitation_24h") > 0, 0.85)
            .otherwise(0.70)
        )
        # --- Alert Flags ---
        .withColumn("immediate_alert",
            F.when(
                (F.col("flood_risk_score") >= 80) |
                ((F.col("precipitation_1h") >= 20) & (F.col("elevation_m") < 100)),
                True
            ).otherwise(False)
        )
        .withColumn("monitoring_required",
            F.when(
                (F.col("flood_risk_score") >= 60) |
                (F.col("precipitation_24h") >= 50),
                True
            ).otherwise(False)
        )
        # ST geospatial: Create evacuation zones based on risk level
        .withColumn("evacuation_zone_radius_m",
            F.when(F.col("flood_risk_category") == "CRITICAL", 5000.0)
            .when(F.col("flood_risk_category") == "HIGH", 3000.0)
            .when(F.col("flood_risk_category") == "MODERATE", 1000.0)
            .otherwise(500.0)
        )
        .withColumn("evacuation_zone", 
            dbf.st_buffer(F.col("geom_point"), F.col("evacuation_zone_radius_m"))
        )
        # Calculate evacuation zone area
        .withColumn("evacuation_zone_area_km2", 
            dbf.st_area(F.col("evacuation_zone")) / 1000000.0
        )
        # Export geometry as GeoJSON for mapping
        .withColumn("location_geojson", dbf.st_asgeojson(F.col("geom_point")))
        .withColumn("evacuation_zone_geojson", dbf.st_asgeojson(F.col("evacuation_zone")))
        .withColumn("processing_timestamp", F.current_timestamp())
    )
    
    return flood_risk


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Flood Risk Summary by Region

# COMMAND ----------

@dlt.table(
    name="gold_flood_risk_regional_summary",
    comment="Gold layer: Regional flood risk aggregations for dashboard and reporting",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_flood_risk_regional_summary():
    """
    Aggregate flood risk scores by region (H3 level 6) for broader analysis.
    """
    flood_scores = dlt.read("gold_flood_risk_scores")
    
    regional_summary = (
        flood_scores
        .groupBy(
            F.col("h3_cell_regional"),
            F.col("country_code")
        )
        .agg(
            F.count("*").alias("total_cells"),
            F.avg("flood_risk_score").alias("avg_flood_risk_score"),
            F.max("flood_risk_score").alias("max_flood_risk_score"),
            F.min("flood_risk_score").alias("min_flood_risk_score"),
            F.avg("terrain_risk_score").alias("avg_terrain_risk"),
            F.avg("weather_risk_score").alias("avg_weather_risk"),
            F.avg("saturation_risk_score").alias("avg_saturation_risk"),
            F.sum(F.when(F.col("flood_risk_category") == "CRITICAL", 1).otherwise(0)).alias("critical_cells"),
            F.sum(F.when(F.col("flood_risk_category") == "HIGH", 1).otherwise(0)).alias("high_risk_cells"),
            F.sum(F.when(F.col("flood_risk_category") == "MODERATE", 1).otherwise(0)).alias("moderate_risk_cells"),
            F.sum(F.when(F.col("immediate_alert"), 1).otherwise(0)).alias("immediate_alert_count"),
            F.sum(F.when(F.col("monitoring_required"), 1).otherwise(0)).alias("monitoring_required_count"),
            F.avg("precipitation_24h").alias("avg_precipitation_24h"),
            F.max("precipitation_24h").alias("max_precipitation_24h"),
            F.avg("elevation_m").alias("avg_elevation_m"),
            F.avg("confidence_score").alias("avg_confidence_score"),
            F.max("observation_time").alias("latest_observation"),
            F.current_timestamp().alias("processing_timestamp")
        )
        .withColumn("regional_risk_category",
            F.when(F.col("avg_flood_risk_score") >= 80, "CRITICAL")
            .when(F.col("avg_flood_risk_score") >= 60, "HIGH")
            .when(F.col("avg_flood_risk_score") >= 40, "MODERATE")
            .when(F.col("avg_flood_risk_score") >= 20, "LOW")
            .otherwise("MINIMAL")
        )
        .withColumn("alert_percentage",
            (F.col("immediate_alert_count") / F.col("total_cells") * 100).cast("decimal(5,2)")
        )
    )
    
    return regional_summary


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Flood Risk Time Series

# COMMAND ----------

@dlt.table(
    name="gold_flood_risk_time_series",
    comment="Gold layer: Time series of flood risk scores for trend analysis",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_flood_risk_time_series():
    """
    Create time series view of flood risk for trending and forecasting.
    """
    flood_scores = dlt.read("gold_flood_risk_scores")
    
    time_series = (
        flood_scores
        .withColumn("observation_date", F.to_date("observation_time"))
        .withColumn("observation_hour", F.hour("observation_time"))
        .groupBy(
            F.col("h3_cell"),
            F.col("location_name"),
            F.col("country_code"),
            F.col("observation_date"),
            F.col("observation_hour")
        )
        .agg(
            F.avg("flood_risk_score").alias("avg_flood_risk_score"),
            F.max("flood_risk_score").alias("max_flood_risk_score"),
            F.avg("precipitation_1h").alias("avg_precipitation_1h"),
            F.sum("precipitation_1h").alias("total_precipitation_1h"),
            F.max("immediate_alert").alias("had_immediate_alert"),
            F.max("monitoring_required").alias("had_monitoring_alert"),
            F.first("latitude").alias("latitude"),
            F.first("longitude").alias("longitude"),
            F.current_timestamp().alias("processing_timestamp")
        )
        # Calculate 6-hour rolling average
        .withColumn("flood_risk_6h_avg",
            F.avg("avg_flood_risk_score").over(
                Window
                .partitionBy("h3_cell")
                .orderBy("observation_date", "observation_hour")
                .rowsBetween(-5, 0)
            )
        )
        # Calculate trend
        .withColumn("risk_trend",
            F.when(F.col("flood_risk_6h_avg") - F.lag("flood_risk_6h_avg", 3).over(
                Window.partitionBy("h3_cell").orderBy("observation_date", "observation_hour")
            ) > 10, "increasing")
            .when(F.col("flood_risk_6h_avg") - F.lag("flood_risk_6h_avg", 3).over(
                Window.partitionBy("h3_cell").orderBy("observation_date", "observation_hour")
            ) < -10, "decreasing")
            .otherwise("stable")
        )
    )
    
    return time_series


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: High Risk Locations Alert Table

# COMMAND ----------

@dlt.table(
    name="gold_flood_high_risk_alerts",
    comment="Gold layer: Active high-risk flood locations requiring immediate attention",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_flood_high_risk_alerts():
    """
    Filter and enrich high-risk locations for alerting systems.
    """
    flood_scores = dlt.read("gold_flood_risk_scores")
    
    alerts = (
        flood_scores
        .filter(
            (F.col("flood_risk_category").isin(["CRITICAL", "HIGH"])) |
            (F.col("immediate_alert") == True)
        )
        .select(
            F.col("h3_cell"),
            F.col("h3_cell_regional"),
            F.col("location_name"),
            F.col("country_code"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("flood_risk_score"),
            F.col("flood_risk_category"),
            F.col("terrain_risk_score"),
            F.col("weather_risk_score"),
            F.col("saturation_risk_score"),
            F.col("elevation_m"),
            F.col("slope_degrees"),
            F.col("precipitation_1h"),
            F.col("precipitation_24h"),
            F.col("soil_saturation"),
            F.col("immediate_alert"),
            F.col("monitoring_required"),
            F.col("confidence_score"),
            F.col("observation_time"),
            F.current_timestamp().alias("alert_timestamp")
        )
        # Add priority ranking
        .withColumn("alert_priority",
            F.when(F.col("immediate_alert"), 1)
            .when(F.col("flood_risk_score") >= 80, 2)
            .when(F.col("flood_risk_score") >= 60, 3)
            .otherwise(4)
        )
        # Add recommended actions
        .withColumn("recommended_action",
            F.when(F.col("immediate_alert"), "EVACUATE: Immediate evacuation recommended for low-lying areas")
            .when(F.col("flood_risk_score") >= 80, "ALERT: Emergency services on standby, monitor continuously")
            .when(F.col("flood_risk_score") >= 60, "WATCH: Prepare emergency response, monitor conditions")
            .otherwise("MONITOR: Continue observation")
        )
        # Add alert ID
        .withColumn("alert_id", 
            F.concat(
                F.col("h3_cell"),
                F.lit("_"),
                F.date_format(F.col("observation_time"), "yyyyMMddHH")
            )
        )
        # Sort by priority
        .orderBy(F.col("alert_priority"), F.col("flood_risk_score").desc())
    )
    
    return alerts


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality and Validation

# COMMAND ----------

@dlt.table(
    name="gold_flood_risk_quality_metrics",
    comment="Gold layer: Data quality metrics for flood risk calculations",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_flood_risk_quality_metrics():
    """
    Monitor data quality and completeness for flood risk pipeline.
    """
    flood_scores = dlt.read("gold_flood_risk_scores")
    
    quality_metrics = (
        flood_scores
        .groupBy("country_code")
        .agg(
            F.count("*").alias("total_locations"),
            F.avg("confidence_score").alias("avg_confidence"),
            F.countDistinct("h3_cell").alias("unique_cells"),
            F.sum(F.when(F.col("elevation_m").isNotNull(), 1).otherwise(0)).alias("terrain_data_available"),
            F.sum(F.when(F.col("precipitation_24h") > 0, 1).otherwise(0)).alias("weather_data_with_precip"),
            F.min("observation_time").alias("earliest_observation"),
            F.max("observation_time").alias("latest_observation"),
            F.current_timestamp().alias("processing_timestamp")
        )
        .withColumn("terrain_coverage_pct",
            (F.col("terrain_data_available") / F.col("total_locations") * 100).cast("decimal(5,2)")
        )
        .withColumn("weather_coverage_pct",
            (F.col("weather_data_with_precip") / F.col("total_locations") * 100).cast("decimal(5,2)")
        )
        .withColumn("data_quality_status",
            F.when(
                (F.col("terrain_coverage_pct") >= 90) & (F.col("avg_confidence") >= 0.85),
                "EXCELLENT"
            ).when(
                (F.col("terrain_coverage_pct") >= 75) & (F.col("avg_confidence") >= 0.70),
                "GOOD"
            ).when(
                (F.col("terrain_coverage_pct") >= 50) & (F.col("avg_confidence") >= 0.50),
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
# MAGIC **Schedule:** Hourly (aligned with weather data updates)
# MAGIC **Target:** Unity Catalog - `demo_hc.climate_risk`
# MAGIC 
# MAGIC **Dependencies:**
# MAGIC - `gold_terrain_features_flood_risk`
# MAGIC - `gold_weather_features_flood_risk`
# MAGIC 
# MAGIC **Outputs:**
# MAGIC - Flood risk scores (H3 cell level)
# MAGIC - Regional summaries
# MAGIC - Time series for trending
# MAGIC - High-risk alerts
# MAGIC - Quality metrics
# MAGIC 
# MAGIC **Alert Thresholds:**
# MAGIC - CRITICAL: Risk score >= 80
# MAGIC - HIGH: Risk score >= 60
# MAGIC - Immediate Alert: Risk >= 80 OR (Precipitation 1h >= 20mm AND Elevation < 100m)

