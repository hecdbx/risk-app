# Databricks notebook source
# MAGIC %md
# MAGIC # Copernicus TIFF Data Source - Usage Examples
# MAGIC 
# MAGIC This notebook demonstrates how to use the custom Copernicus TIFF data source.
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC 1. Run `copernicus_tiff_datasource.py` to register the data source
# MAGIC 2. Upload TIFF files to Unity Catalog volume
# MAGIC 3. Ensure Unity Catalog is configured

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ./copernicus_tiff_datasource

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Basic TIFF Ingestion

# COMMAND ----------

# Define paths
volume_path = "/Volumes/demo_hc/climate_risk/terrain_data/"
tiff_file = f"{volume_path}copernicus_dem_sample.tif"

# Read TIFF data
df = spark.read.format("copernicus_tiff") \
    .option("h3_resolution", "8") \
    .option("sampling_interval", "10") \
    .option("max_rows", "1000") \
    .load(tiff_file)

# Display results
print("📊 TIFF Data Preview:")
df.show(10, truncate=False)

# Show schema
print("\n📋 Schema:")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Elevation Statistics

# COMMAND ----------

# Calculate elevation statistics
stats_df = df.select("elevation_m").summary("count", "mean", "min", "max", "stddev")
print("📈 Elevation Statistics:")
stats_df.show()

# Elevation distribution
print("\n📊 Elevation Distribution:")
df.groupBy(
    (F.floor(F.col("elevation_m") / 100) * 100).cast("int").alias("elevation_bin")
).count() \
.orderBy("elevation_bin") \
.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: H3 Spatial Aggregation

# COMMAND ----------

# Aggregate by H3 cell
h3_agg_df = df.groupBy("h3_cell_8", "h3_cell_6") \
    .agg(
        F.avg("elevation_m").alias("avg_elevation_m"),
        F.min("elevation_m").alias("min_elevation_m"),
        F.max("elevation_m").alias("max_elevation_m"),
        F.stddev("elevation_m").alias("stddev_elevation_m"),
        F.count("*").alias("pixel_count"),
        F.first("longitude").alias("longitude"),
        F.first("latitude").alias("latitude")
    ) \
    .withColumn("elevation_range_m", F.col("max_elevation_m") - F.col("min_elevation_m"))

print("🗺️ H3 Aggregated Data:")
h3_agg_df.orderBy(F.desc("avg_elevation_m")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Save to Unity Catalog

# COMMAND ----------

# Save raw data to Bronze layer
print("💾 Saving to Bronze layer...")
df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("demo_hc.climate_risk.bronze_copernicus_dem")

print("✅ Data saved to demo_hc.climate_risk.bronze_copernicus_dem")

# Save aggregated data to Silver layer
print("\n💾 Saving aggregated data to Silver layer...")
h3_agg_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("demo_hc.climate_risk.silver_terrain_h3_aggregated")

print("✅ Aggregated data saved to demo_hc.climate_risk.silver_terrain_h3_aggregated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Query from Unity Catalog

# COMMAND ----------

# Query the bronze table
bronze_df = spark.table("demo_hc.climate_risk.bronze_copernicus_dem")

print(f"📊 Bronze table row count: {bronze_df.count():,}")
print("\nSample data:")
bronze_df.select("longitude", "latitude", "elevation_m", "h3_cell_8").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 6: Spatial Analysis

# COMMAND ----------

# Find high elevation areas (> 1000m)
high_elevation_df = bronze_df.filter(F.col("elevation_m") > 1000)

print(f"🏔️ High elevation areas (>1000m): {high_elevation_df.count():,} pixels")
print("\nTop 10 highest points:")
high_elevation_df.orderBy(F.desc("elevation_m")) \
    .select("longitude", "latitude", "elevation_m", "h3_cell_8") \
    .show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 7: Batch Processing Multiple Files

# COMMAND ----------

# List all TIFF files in volume
try:
    tiff_files = [f for f in dbutils.fs.ls(volume_path) if f.name.lower().endswith(('.tif', '.tiff'))]
    
    print(f"📁 Found {len(tiff_files)} TIFF files:")
    for f in tiff_files:
        print(f"  - {f.name} ({f.size / 1024 / 1024:.2f} MB)")
    
    # Process each file
    if len(tiff_files) > 0:
        for tiff_file in tiff_files[:3]:  # Process first 3 files as example
            print(f"\n🔄 Processing: {tiff_file.name}")
            
            df = spark.read.format("copernicus_tiff") \
                .option("h3_resolution", "8") \
                .option("sampling_interval", "15") \
                .option("max_rows", "500") \
                .load(tiff_file.path)
            
            row_count = df.count()
            print(f"  ✅ Processed {row_count:,} rows from {tiff_file.name}")
            
            # Append to existing table
            df.write.format("delta") \
                .mode("append") \
                .saveAsTable("demo_hc.climate_risk.bronze_copernicus_dem")
            
except Exception as e:
    print(f"⚠️ Note: {str(e)}")
    print("Make sure TIFF files are uploaded to the volume first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 8: Integration with Existing Pipeline

# COMMAND ----------

# Read from bronze table
bronze_data = spark.table("demo_hc.climate_risk.bronze_copernicus_dem")

# Apply transformations similar to existing pipeline
transformed_df = bronze_data \
    .withColumn("elevation_category",
        F.when(F.col("elevation_m") < 100, "lowland")
        .when(F.col("elevation_m") < 500, "hills")
        .when(F.col("elevation_m") < 1500, "mountains")
        .otherwise("high_mountains")
    ) \
    .withColumn("flood_risk_indicator",
        F.when(F.col("elevation_m") < 50, "high")
        .when(F.col("elevation_m") < 200, "moderate")
        .otherwise("low")
    )

print("🔄 Transformed Data:")
transformed_df.groupBy("elevation_category", "flood_risk_indicator").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 9: Data Quality Checks

# COMMAND ----------

# Check for null values
null_check = bronze_data.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in bronze_data.columns
])

print("🔍 Null Value Check:")
null_check.show()

# Check elevation range
print("\n📊 Elevation Range Check:")
bronze_data.select(
    F.min("elevation_m").alias("min_elevation"),
    F.max("elevation_m").alias("max_elevation"),
    F.avg("elevation_m").alias("avg_elevation"),
    F.count("*").alias("total_pixels")
).show()

# Check H3 coverage
print("\n🗺️ H3 Coverage:")
print(f"Unique H3 cells (level 8): {bronze_data.select('h3_cell_8').distinct().count():,}")
print(f"Unique H3 cells (level 6): {bronze_data.select('h3_cell_6').distinct().count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook demonstrated:
# MAGIC 
# MAGIC ✅ Basic TIFF file ingestion  
# MAGIC ✅ Elevation statistics and analysis  
# MAGIC ✅ H3 spatial aggregation  
# MAGIC ✅ Saving data to Unity Catalog (Bronze and Silver layers)  
# MAGIC ✅ Querying from Unity Catalog  
# MAGIC ✅ Spatial analysis (high elevation areas)  
# MAGIC ✅ Batch processing multiple files  
# MAGIC ✅ Integration with existing pipeline  
# MAGIC ✅ Data quality checks  
# MAGIC 
# MAGIC The custom data source is now ready for production use in the climate risk pipeline!

