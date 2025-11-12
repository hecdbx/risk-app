# Databricks notebook source
# MAGIC %md
# MAGIC # AccuWeather European Locations Ingestion Pipeline
# MAGIC 
# MAGIC This notebook implements real-time weather data ingestion from AccuWeather API
# MAGIC for European cities and locations to support flood and drought risk calculations.
# MAGIC 
# MAGIC **Data Coverage:**
# MAGIC - Major European capitals and cities
# MAGIC - Real-time current conditions
# MAGIC - Historical weather data (past 24 hours)
# MAGIC - 5-day forecasts with precipitation details

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import dlt
import requests
import json
import yaml
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.databricks.sql import functions as dbf  # Databricks ST functions
import pandas as pd
from typing import List, Dict, Optional
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Secrets

# COMMAND ----------

# Load configuration
CONFIG_PATH = "/Workspace/Shared/risk_app/config/european_data_sources.yaml"

try:
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
        EUROPEAN_LOCATIONS = config['weather_data']['accuweather_europe']['european_capitals']
except:
    # Fallback European capitals configuration
    EUROPEAN_LOCATIONS = [
        {"key": "178087", "name": "Paris, France", "lat": 48.8566, "lon": 2.3522, "country": "FR"},
        {"key": "308526", "name": "Berlin, Germany", "lat": 52.5200, "lon": 13.4050, "country": "DE"},
        {"key": "308084", "name": "Madrid, Spain", "lat": 40.4168, "lon": -3.7038, "country": "ES"},
        {"key": "213490", "name": "Rome, Italy", "lat": 41.9028, "lon": 12.4964, "country": "IT"},
        {"key": "274087", "name": "Amsterdam, Netherlands", "lat": 52.3676, "lon": 4.9041, "country": "NL"},
        {"key": "264885", "name": "Brussels, Belgium", "lat": 50.8503, "lon": 4.3517, "country": "BE"},
        {"key": "315078", "name": "Vienna, Austria", "lat": 48.2082, "lon": 16.3738, "country": "AT"},
        {"key": "264379", "name": "Warsaw, Poland", "lat": 52.2297, "lon": 21.0122, "country": "PL"},
        {"key": "324505", "name": "Bucharest, Romania", "lat": 44.4268, "lon": 26.1025, "country": "RO"},
        {"key": "324582", "name": "Athens, Greece", "lat": 37.9838, "lon": 23.7275, "country": "GR"},
        {"key": "328328", "name": "Stockholm, Sweden", "lat": 59.3293, "lon": 18.0686, "country": "SE"},
        {"key": "316665", "name": "Copenhagen, Denmark", "lat": 55.6761, "lon": 12.5683, "country": "DK"},
        {"key": "318849", "name": "Helsinki, Finland", "lat": 60.1699, "lon": 24.9384, "country": "FI"},
        {"key": "274456", "name": "Lisbon, Portugal", "lat": 38.7223, "lon": -9.1393, "country": "PT"},
        {"key": "264171", "name": "Dublin, Ireland", "lat": 53.3498, "lon": -6.2603, "country": "IE"}
    ]

# AccuWeather API configuration
try:
    ACCUWEATHER_API_KEY = dbutils.secrets.get(scope="accuweather", key="api_key")
except:
    ACCUWEATHER_API_KEY = spark.conf.get("accuweather.api.key", "DEMO_KEY")

ACCUWEATHER_BASE_URL = "http://dataservice.accuweather.com"

# Rate limiting configuration
API_CALLS_PER_MINUTE = 50
DELAY_BETWEEN_CALLS = 60.0 / API_CALLS_PER_MINUTE  # seconds

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions for AccuWeather API

# COMMAND ----------

def fetch_current_conditions(location_key: str, location_name: str, lat: float, lon: float, country: str) -> Dict:
    """
    Fetch current weather conditions from AccuWeather API.
    
    Args:
        location_key: AccuWeather location key
        location_name: Human-readable location name
        lat: Latitude
        lon: Longitude
        country: Country code
    
    Returns:
        Dictionary with current weather data
    """
    try:
        url = f"{ACCUWEATHER_BASE_URL}/currentconditions/v1/{location_key}"
        params = {
            "apikey": ACCUWEATHER_API_KEY,
            "details": "true"
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if data and len(data) > 0:
            current = data[0]
            
            return {
                "location_key": location_key,
                "location_name": location_name,
                "country_code": country,
                "latitude": lat,
                "longitude": lon,
                "observation_time": datetime.fromtimestamp(current.get("EpochTime", time.time())),
                "weather_text": current.get("WeatherText", ""),
                "weather_icon": current.get("WeatherIcon", 0),
                "has_precipitation": current.get("HasPrecipitation", False),
                "precipitation_type": current.get("PrecipitationType", None),
                "temperature_celsius": current.get("Temperature", {}).get("Metric", {}).get("Value", None),
                "temperature_fahrenheit": current.get("Temperature", {}).get("Imperial", {}).get("Value", None),
                "realfeel_celsius": current.get("RealFeelTemperature", {}).get("Metric", {}).get("Value", None),
                "humidity_percent": current.get("RelativeHumidity", None),
                "dewpoint_celsius": current.get("DewPoint", {}).get("Metric", {}).get("Value", None),
                "wind_speed_kmh": current.get("Wind", {}).get("Speed", {}).get("Metric", {}).get("Value", None),
                "wind_direction_degrees": current.get("Wind", {}).get("Direction", {}).get("Degrees", None),
                "wind_direction_text": current.get("Wind", {}).get("Direction", {}).get("English", None),
                "wind_gust_kmh": current.get("WindGust", {}).get("Speed", {}).get("Metric", {}).get("Value", None),
                "pressure_mb": current.get("Pressure", {}).get("Metric", {}).get("Value", None),
                "pressure_tendency": current.get("PressureTendency", {}).get("LocalizedText", None),
                "visibility_km": current.get("Visibility", {}).get("Metric", {}).get("Value", None),
                "cloud_cover_percent": current.get("CloudCover", None),
                "ceiling_m": current.get("Ceiling", {}).get("Metric", {}).get("Value", None),
                "uv_index": current.get("UVIndex", None),
                "uv_index_text": current.get("UVIndexText", None),
                "precipitation_summary_1h": current.get("PrecipitationSummary", {}).get("Past1Hour", {}).get("Metric", {}).get("Value", 0.0),
                "precipitation_summary_3h": current.get("PrecipitationSummary", {}).get("Past3Hours", {}).get("Metric", {}).get("Value", 0.0),
                "precipitation_summary_6h": current.get("PrecipitationSummary", {}).get("Past6Hours", {}).get("Metric", {}).get("Value", 0.0),
                "precipitation_summary_9h": current.get("PrecipitationSummary", {}).get("Past9Hours", {}).get("Metric", {}).get("Value", 0.0),
                "precipitation_summary_12h": current.get("PrecipitationSummary", {}).get("Past12Hours", {}).get("Metric", {}).get("Value", 0.0),
                "precipitation_summary_18h": current.get("PrecipitationSummary", {}).get("Past18Hours", {}).get("Metric", {}).get("Value", 0.0),
                "precipitation_summary_24h": current.get("PrecipitationSummary", {}).get("Past24Hours", {}).get("Metric", {}).get("Value", 0.0),
                "ingestion_timestamp": datetime.now()
            }
    except Exception as e:
        print(f"Error fetching current conditions for {location_name}: {str(e)}")
        return None


def fetch_daily_forecast(location_key: str, location_name: str, lat: float, lon: float, country: str) -> List[Dict]:
    """
    Fetch 5-day daily forecast from AccuWeather API.
    
    Args:
        location_key: AccuWeather location key
        location_name: Human-readable location name
        lat: Latitude
        lon: Longitude
        country: Country code
    
    Returns:
        List of dictionaries with forecast data
    """
    try:
        url = f"{ACCUWEATHER_BASE_URL}/forecasts/v1/daily/5day/{location_key}"
        params = {
            "apikey": ACCUWEATHER_API_KEY,
            "details": "true",
            "metric": "true"
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        forecasts = []
        
        if data and "DailyForecasts" in data:
            for daily in data["DailyForecasts"]:
                forecast_record = {
                    "location_key": location_key,
                    "location_name": location_name,
                    "country_code": country,
                    "latitude": lat,
                    "longitude": lon,
                    "forecast_date": datetime.fromtimestamp(daily.get("EpochDate", 0)).date(),
                    "temperature_min_celsius": daily.get("Temperature", {}).get("Minimum", {}).get("Value", None),
                    "temperature_max_celsius": daily.get("Temperature", {}).get("Maximum", {}).get("Value", None),
                    "realfeel_min_celsius": daily.get("RealFeelTemperature", {}).get("Minimum", {}).get("Value", None),
                    "realfeel_max_celsius": daily.get("RealFeelTemperature", {}).get("Maximum", {}).get("Value", None),
                    "day_icon": daily.get("Day", {}).get("Icon", None),
                    "day_icon_phrase": daily.get("Day", {}).get("IconPhrase", None),
                    "day_has_precipitation": daily.get("Day", {}).get("HasPrecipitation", False),
                    "day_precipitation_type": daily.get("Day", {}).get("PrecipitationType", None),
                    "day_precipitation_intensity": daily.get("Day", {}).get("PrecipitationIntensity", None),
                    "night_icon": daily.get("Night", {}).get("Icon", None),
                    "night_icon_phrase": daily.get("Night", {}).get("IconPhrase", None),
                    "night_has_precipitation": daily.get("Night", {}).get("HasPrecipitation", False),
                    "night_precipitation_type": daily.get("Night", {}).get("PrecipitationType", None),
                    "night_precipitation_intensity": daily.get("Night", {}).get("PrecipitationIntensity", None),
                    "hours_of_sun": daily.get("HoursOfSun", None),
                    "solar_irradiance": daily.get("AirAndPollen", [{}])[0].get("Value", None) if daily.get("AirAndPollen") else None,
                    "ingestion_timestamp": datetime.now()
                }
                forecasts.append(forecast_record)
        
        return forecasts
        
    except Exception as e:
        print(f"Error fetching forecast for {location_name}: {str(e)}")
        return []


def fetch_all_european_weather() -> tuple:
    """
    Fetch current conditions and forecasts for all European locations.
    
    Returns:
        Tuple of (current_conditions_list, forecasts_list)
    """
    current_conditions = []
    forecasts = []
    
    for location in EUROPEAN_LOCATIONS:
        # Fetch current conditions
        current = fetch_current_conditions(
            location["key"],
            location["name"],
            location["lat"],
            location["lon"],
            location["country"]
        )
        if current:
            current_conditions.append(current)
        
        # Rate limiting
        time.sleep(DELAY_BETWEEN_CALLS)
        
        # Fetch forecasts
        forecast = fetch_daily_forecast(
            location["key"],
            location["name"],
            location["lat"],
            location["lon"],
            location["country"]
        )
        if forecast:
            forecasts.extend(forecast)
        
        # Rate limiting
        time.sleep(DELAY_BETWEEN_CALLS)
    
    return current_conditions, forecasts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw AccuWeather Data

# COMMAND ----------

@dlt.table(
    name="bronze_accuweather_europe_current",
    comment="Bronze layer: Raw current weather conditions for European locations",
    table_properties={
        "quality": "bronze",
        "data_source": "AccuWeather Europe",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_accuweather_europe_current():
    """
    Ingest current weather conditions from AccuWeather for European locations.
    """
    # Fetch weather data
    current_conditions, _ = fetch_all_european_weather()
    
    if not current_conditions:
        # Return empty DataFrame with schema
        schema = StructType([
            StructField("location_key", StringType(), False),
            StructField("location_name", StringType(), False),
            StructField("country_code", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("observation_time", TimestampType(), False),
            StructField("weather_text", StringType(), True),
            StructField("weather_icon", IntegerType(), True),
            StructField("has_precipitation", BooleanType(), True),
            StructField("precipitation_type", StringType(), True),
            StructField("temperature_celsius", DoubleType(), True),
            StructField("temperature_fahrenheit", DoubleType(), True),
            StructField("realfeel_celsius", DoubleType(), True),
            StructField("humidity_percent", IntegerType(), True),
            StructField("dewpoint_celsius", DoubleType(), True),
            StructField("wind_speed_kmh", DoubleType(), True),
            StructField("wind_direction_degrees", IntegerType(), True),
            StructField("wind_direction_text", StringType(), True),
            StructField("wind_gust_kmh", DoubleType(), True),
            StructField("pressure_mb", DoubleType(), True),
            StructField("pressure_tendency", StringType(), True),
            StructField("visibility_km", DoubleType(), True),
            StructField("cloud_cover_percent", IntegerType(), True),
            StructField("ceiling_m", DoubleType(), True),
            StructField("uv_index", IntegerType(), True),
            StructField("uv_index_text", StringType(), True),
            StructField("precipitation_summary_1h", DoubleType(), True),
            StructField("precipitation_summary_3h", DoubleType(), True),
            StructField("precipitation_summary_6h", DoubleType(), True),
            StructField("precipitation_summary_9h", DoubleType(), True),
            StructField("precipitation_summary_12h", DoubleType(), True),
            StructField("precipitation_summary_18h", DoubleType(), True),
            StructField("precipitation_summary_24h", DoubleType(), True),
            StructField("ingestion_timestamp", TimestampType(), False)
        ])
        return spark.createDataFrame([], schema)
    
    # Convert to DataFrame
    df = spark.createDataFrame(current_conditions)
    return df


@dlt.table(
    name="bronze_accuweather_europe_forecast",
    comment="Bronze layer: Raw 5-day forecast data for European locations",
    table_properties={
        "quality": "bronze",
        "data_source": "AccuWeather Europe",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_accuweather_europe_forecast():
    """
    Ingest 5-day forecast data from AccuWeather for European locations.
    """
    # Fetch weather data
    _, forecasts = fetch_all_european_weather()
    
    if not forecasts:
        # Return empty DataFrame with schema
        schema = StructType([
            StructField("location_key", StringType(), False),
            StructField("location_name", StringType(), False),
            StructField("country_code", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("forecast_date", DateType(), False),
            StructField("temperature_min_celsius", DoubleType(), True),
            StructField("temperature_max_celsius", DoubleType(), True),
            StructField("realfeel_min_celsius", DoubleType(), True),
            StructField("realfeel_max_celsius", DoubleType(), True),
            StructField("day_icon", IntegerType(), True),
            StructField("day_icon_phrase", StringType(), True),
            StructField("day_has_precipitation", BooleanType(), True),
            StructField("day_precipitation_type", StringType(), True),
            StructField("day_precipitation_intensity", StringType(), True),
            StructField("night_icon", IntegerType(), True),
            StructField("night_icon_phrase", StringType(), True),
            StructField("night_has_precipitation", BooleanType(), True),
            StructField("night_precipitation_type", StringType(), True),
            StructField("night_precipitation_intensity", StringType(), True),
            StructField("hours_of_sun", DoubleType(), True),
            StructField("solar_irradiance", DoubleType(), True),
            StructField("ingestion_timestamp", TimestampType(), False)
        ])
        return spark.createDataFrame([], schema)
    
    # Convert to DataFrame
    df = spark.createDataFrame(forecasts)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Enriched Weather Data

# COMMAND ----------

@dlt.table(
    name="silver_weather_europe_enriched",
    comment="Silver layer: Enriched European weather data with H3 indexing and quality scores",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop({"valid_temperature": "temperature_celsius BETWEEN -50 AND 50"})
@dlt.expect_all_or_drop({"valid_coordinates": "latitude BETWEEN 35 AND 72 AND longitude BETWEEN -25 AND 45"})
@dlt.expect_all_or_drop({"valid_timestamp": "observation_time IS NOT NULL"})
def silver_weather_europe_enriched():
    """
    Enriched weather data with spatial indexing and derived metrics.
    """
    return (
        dlt.read("bronze_accuweather_europe_current")
        # H3 spatial indexing at multiple resolutions
        .withColumn("h3_cell_6", F.expr("h3_latlng_to_cell_string(latitude, longitude, 6)"))
        .withColumn("h3_cell_8", F.expr("h3_latlng_to_cell_string(latitude, longitude, 8)"))
        .withColumn("h3_cell_10", F.expr("h3_latlng_to_cell_string(latitude, longitude, 10)"))
        # ST geospatial functions - create point geometries
        .withColumn("geom_point", dbf.st_point(F.col("longitude"), F.col("latitude")))
        .withColumn("geog_point", dbf.st_geogfromtext(
            F.concat(F.lit("POINT("), F.col("longitude"), F.lit(" "), F.col("latitude"), F.lit(")"))
        ))
        # Create buffer zones for spatial analysis (5km, 10km, 25km radii)
        .withColumn("buffer_5km", dbf.st_buffer(F.col("geom_point"), 5000.0))
        .withColumn("buffer_10km", dbf.st_buffer(F.col("geom_point"), 10000.0))
        .withColumn("buffer_25km", dbf.st_buffer(F.col("geom_point"), 25000.0))
        # Data quality scoring
        .withColumn("data_completeness_score",
            F.when(F.col("temperature_celsius").isNull(), 0.0)
            .when(F.col("humidity_percent").isNull(), 0.7)
            .when(F.col("precipitation_summary_24h").isNull(), 0.8)
            .otherwise(1.0)
        )
        # Derived weather metrics
        .withColumn("heat_index_celsius",
            F.when(F.col("temperature_celsius") > 26,
                F.col("temperature_celsius") + (0.5 * (F.col("humidity_percent") / 100.0 - 0.1))
            ).otherwise(F.col("temperature_celsius"))
        )
        .withColumn("wind_chill_celsius",
            F.when(F.col("temperature_celsius") < 10,
                13.12 + 0.6215 * F.col("temperature_celsius") - 
                11.37 * F.pow(F.col("wind_speed_kmh"), 0.16) + 
                0.3965 * F.col("temperature_celsius") * F.pow(F.col("wind_speed_kmh"), 0.16)
            ).otherwise(F.col("temperature_celsius"))
        )
        # Precipitation classification
        .withColumn("precipitation_24h_class",
            F.when(F.col("precipitation_summary_24h") >= 100, "extreme")
            .when(F.col("precipitation_summary_24h") >= 50, "heavy")
            .when(F.col("precipitation_summary_24h") >= 10, "moderate")
            .when(F.col("precipitation_summary_24h") >= 2.5, "light")
            .otherwise("none")
        )
        # Weather severity
        .withColumn("weather_severity",
            F.when(F.col("weather_icon").isin([15, 16, 17, 18]), "severe")  # Thunderstorms
            .when(F.col("weather_icon").isin([12, 13, 14]), "moderate")     # Rain/showers
            .when(F.col("weather_icon").isin([7, 8, 11]), "light_precip")   # Clouds/drizzle
            .otherwise("normal")
        )
        # Flood risk indicators
        .withColumn("flood_risk_indicator",
            F.when(F.col("precipitation_summary_24h") > 50, "high")
            .when(F.col("precipitation_summary_12h") > 30, "moderate")
            .when(F.col("precipitation_summary_6h") > 20, "low")
            .otherwise("none")
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Weather Features for Risk Modeling

# COMMAND ----------

@dlt.table(
    name="gold_weather_features_flood_risk",
    comment="Gold layer: Weather features optimized for flood risk modeling",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_weather_features_flood_risk():
    """
    Weather features specifically for flood risk assessment.
    """
    weather = dlt.read("silver_weather_europe_enriched")
    
    return (
        weather
        .select(
            F.col("location_key"),
            F.col("location_name"),
            F.col("country_code"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("h3_cell_6"),
            F.col("h3_cell_8"),
            F.col("observation_time"),
            F.col("temperature_celsius"),
            F.col("humidity_percent"),
            F.col("precipitation_summary_1h"),
            F.col("precipitation_summary_6h"),
            F.col("precipitation_summary_12h"),
            F.col("precipitation_summary_24h"),
            F.col("precipitation_24h_class"),
            F.col("flood_risk_indicator"),
            F.col("weather_severity")
        )
        # Cumulative precipitation metrics
        .withColumn("precip_intensity_1h", F.col("precipitation_summary_1h"))
        .withColumn("precip_intensity_6h", F.col("precipitation_summary_6h") / 6.0)
        .withColumn("precip_intensity_24h", F.col("precipitation_summary_24h") / 24.0)
        # Soil saturation proxy
        .withColumn("soil_saturation_proxy",
            F.least(
                (F.col("precipitation_summary_24h") / 100.0) * 0.7 + 
                (F.col("humidity_percent") / 100.0) * 0.3,
                F.lit(1.0)
            )
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )


@dlt.table(
    name="gold_weather_features_drought_risk",
    comment="Gold layer: Weather features optimized for drought risk modeling",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_weather_features_drought_risk():
    """
    Weather features specifically for drought risk assessment.
    """
    weather = dlt.read("silver_weather_europe_enriched")
    
    # Calculate 7-day and 30-day rolling precipitation (simplified - would need historical data)
    return (
        weather
        .select(
            F.col("location_key"),
            F.col("location_name"),
            F.col("country_code"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("h3_cell_6"),
            F.col("h3_cell_8"),
            F.col("observation_time"),
            F.col("temperature_celsius"),
            F.col("humidity_percent"),
            F.col("precipitation_summary_24h"),
            F.col("dewpoint_celsius")
        )
        # Drought indicators
        .withColumn("consecutive_dry_days_estimate",
            F.when(F.col("precipitation_summary_24h") < 1.0, 1).otherwise(0)
        )
        .withColumn("evapotranspiration_potential_mm",
            # Simplified Penman equation proxy
            F.greatest(
                0.0,
                0.0023 * (F.col("temperature_celsius") + 17.8) * 
                F.sqrt(F.greatest(0, F.col("temperature_celsius") - F.col("dewpoint_celsius")))
            )
        )
        .withColumn("water_deficit_daily",
            F.col("evapotranspiration_potential_mm") - F.col("precipitation_summary_24h")
        )
        .withColumn("drought_stress_indicator",
            F.when(F.col("water_deficit_daily") > 10, "high")
            .when(F.col("water_deficit_daily") > 5, "moderate")
            .when(F.col("water_deficit_daily") > 0, "low")
            .otherwise("none")
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Monitoring

# COMMAND ----------

@dlt.table(
    name="gold_weather_pipeline_metrics",
    comment="Gold layer: Pipeline execution metrics for weather data",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_weather_pipeline_metrics():
    """
    Monitor pipeline execution and data quality.
    """
    weather = dlt.read("silver_weather_europe_enriched")
    
    metrics = (
        weather
        .groupBy("country_code")
        .agg(
            F.count("*").alias("total_observations"),
            F.avg("temperature_celsius").alias("avg_temperature"),
            F.avg("precipitation_summary_24h").alias("avg_precipitation_24h"),
            F.max("precipitation_summary_24h").alias("max_precipitation_24h"),
            F.sum(F.when(F.col("flood_risk_indicator") == "high", 1).otherwise(0)).alias("high_flood_risk_count"),
            F.avg("data_completeness_score").alias("avg_data_quality"),
            F.max("observation_time").alias("latest_observation"),
            F.current_timestamp().alias("processing_timestamp")
        )
        .withColumn("data_completeness_percent", 
            (F.col("avg_data_quality") * 100).cast("int")
        )
    )
    
    return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration
# MAGIC 
# MAGIC **Execution Mode:** Triggered or Scheduled
# MAGIC **Schedule:** Hourly (for real-time weather monitoring)
# MAGIC **Target:** Unity Catalog - `demo_hc.climate_risk`
# MAGIC 
# MAGIC **API Configuration:**
# MAGIC - Rate Limit: 50 calls/minute (free tier) or 25,000 calls/day (standard)
# MAGIC - Locations: 15 European capitals
# MAGIC - Update Frequency: Hourly for current conditions
# MAGIC 
# MAGIC **Data Quality:**
# MAGIC - Automated quality checks on temperature, precipitation, coordinates
# MAGIC - Invalid data automatically dropped
# MAGIC - Data freshness monitoring

