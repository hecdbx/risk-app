# Databricks notebook source
# MAGIC %md
# MAGIC # Copernicus TIFF Custom Data Source
# MAGIC 
# MAGIC This notebook implements a custom PySpark DataSource for ingesting Copernicus DEM TIFF files
# MAGIC into Unity Catalog volumes with spatial indexing using H3.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Reads TIFF files from Unity Catalog volumes or cloud storage
# MAGIC - Extracts elevation and terrain data
# MAGIC - Applies H3 spatial indexing
# MAGIC - Supports batch processing of multiple TIFF files
# MAGIC - Handles metadata extraction
# MAGIC - CRS transformation to WGS84

# COMMAND ----------

# MAGIC %pip install rasterio h3 numpy --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Setup

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType, BinaryType
from pyspark.sql import Row
from typing import Iterator, Tuple, List, Optional
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copernicus TIFF DataSource Definition

# COMMAND ----------

class CopernicusTiffDataSource(DataSource):
    """
    Custom PySpark DataSource for reading Copernicus DEM TIFF files.
    
    This data source reads TIFF files containing Digital Elevation Model (DEM) data
    and extracts elevation values along with spatial metadata.
    
    Options:
        - path: Path to TIFF file(s) - can be Unity Catalog volume path or cloud storage
        - h3_resolution: H3 resolution level (default: 8)
        - sampling_interval: Pixel sampling interval (default: 10)
        - extract_metadata: Whether to extract TIFF metadata (default: true)
        - target_crs: Target CRS for coordinates (default: EPSG:4326)
    
    Example usage:
        spark.read.format("copernicus_tiff") \\
            .option("h3_resolution", "8") \\
            .option("sampling_interval", "10") \\
            .load("/Volumes/demo_hc/climate_risk/terrain_data/copernicus_dem.tif")
    """
    
    @classmethod
    def name(cls):
        return "copernicus_tiff"
    
    def schema(self):
        """
        Define the schema for the TIFF data output.
        Returns elevation data with coordinates, H3 index, and metadata.
        """
        return StructType([
            StructField("file_path", StringType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("elevation_m", FloatType(), True),
            StructField("h3_cell_8", StringType(), True),
            StructField("h3_cell_6", StringType(), True),
            StructField("pixel_x", IntegerType(), False),
            StructField("pixel_y", IntegerType(), False),
            StructField("crs", StringType(), True),
            StructField("nodata_value", FloatType(), True),
            StructField("resolution_x", DoubleType(), True),
            StructField("resolution_y", DoubleType(), True),
            StructField("bounds_minx", DoubleType(), True),
            StructField("bounds_miny", DoubleType(), True),
            StructField("bounds_maxx", DoubleType(), True),
            StructField("bounds_maxy", DoubleType(), True),
            StructField("processing_timestamp", StringType(), False)
        ])
    
    def reader(self, schema: StructType):
        return CopernicusTiffReader(schema, self.options)


# COMMAND ----------

# MAGIC %md
# MAGIC ## TIFF Reader Implementation

# COMMAND ----------

class CopernicusTiffReader(DataSourceReader):
    """
    Reader implementation for Copernicus TIFF files.
    
    Reads TIFF files, extracts elevation data, and applies spatial indexing.
    """
    
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        
        # Parse options with defaults
        self.h3_resolution = int(options.get("h3_resolution", "8"))
        self.h3_resolution_regional = int(options.get("h3_resolution_regional", "6"))
        self.sampling_interval = int(options.get("sampling_interval", "10"))
        self.extract_metadata = options.get("extract_metadata", "true").lower() == "true"
        self.target_crs = options.get("target_crs", "EPSG:4326")
        self.max_rows = int(options.get("max_rows", "-1"))  # -1 means no limit
        
    def read(self, partition) -> Iterator[Tuple]:
        """
        Read TIFF file and yield rows of elevation data with spatial information.
        
        This method processes TIFF files pixel by pixel (with sampling) and extracts:
        - Coordinates (lon/lat)
        - Elevation values
        - H3 spatial indices
        - Metadata
        """
        # Import libraries inside the method for serialization
        import rasterio
        from rasterio.warp import transform
        import h3
        import numpy as np
        from datetime import datetime
        
        # Get the file path from options
        file_path = self.options.get("path")
        if not file_path:
            raise ValueError("Must specify a file path using .load() or .option('path', '...')")
        
        try:
            # Open the TIFF file
            with rasterio.open(file_path) as src:
                # Extract metadata
                source_crs = src.crs.to_string() if src.crs else "Unknown"
                nodata = src.nodata if src.nodata is not None else -9999.0
                resolution_x, resolution_y = src.res
                bounds = src.bounds
                
                # Read the elevation data
                elevation_data = src.read(1)  # Read first band
                
                # Get image dimensions
                height, width = elevation_data.shape
                
                # Prepare coordinate transformation if needed
                transform_needed = source_crs != self.target_crs
                
                processing_timestamp = datetime.utcnow().isoformat()
                
                row_count = 0
                
                # Sample pixels at specified interval
                for y in range(0, height, self.sampling_interval):
                    for x in range(0, width, self.sampling_interval):
                        # Check max rows limit
                        if self.max_rows > 0 and row_count >= self.max_rows:
                            return
                        
                        # Get elevation value
                        elevation = float(elevation_data[y, x])
                        
                        # Skip nodata values
                        if elevation == nodata or np.isnan(elevation):
                            continue
                        
                        # Convert pixel coordinates to geographic coordinates
                        lon_src, lat_src = src.xy(y, x)
                        
                        # Transform coordinates if necessary
                        if transform_needed:
                            try:
                                lon, lat = transform(
                                    source_crs,
                                    self.target_crs,
                                    [lon_src],
                                    [lat_src]
                                )
                                lon, lat = lon[0], lat[0]
                            except Exception as e:
                                # If transformation fails, use source coordinates
                                lon, lat = lon_src, lat_src
                        else:
                            lon, lat = lon_src, lat_src
                        
                        # Calculate H3 indices
                        try:
                            h3_cell_8 = h3.latlng_to_cell(lat, lon, self.h3_resolution)
                            h3_cell_6 = h3.latlng_to_cell(lat, lon, self.h3_resolution_regional)
                        except Exception:
                            h3_cell_8 = None
                            h3_cell_6 = None
                        
                        # Yield row
                        yield (
                            file_path,                          # file_path
                            float(lon),                         # longitude
                            float(lat),                         # latitude
                            float(elevation),                   # elevation_m
                            h3_cell_8,                          # h3_cell_8
                            h3_cell_6,                          # h3_cell_6
                            int(x),                            # pixel_x
                            int(y),                            # pixel_y
                            source_crs if self.extract_metadata else None,  # crs
                            float(nodata) if self.extract_metadata else None,  # nodata_value
                            float(resolution_x) if self.extract_metadata else None,  # resolution_x
                            float(resolution_y) if self.extract_metadata else None,  # resolution_y
                            float(bounds.left) if self.extract_metadata else None,  # bounds_minx
                            float(bounds.bottom) if self.extract_metadata else None,  # bounds_miny
                            float(bounds.right) if self.extract_metadata else None,  # bounds_maxx
                            float(bounds.top) if self.extract_metadata else None,  # bounds_maxy
                            processing_timestamp               # processing_timestamp
                        )
                        
                        row_count += 1
        
        except Exception as e:
            raise Exception(f"Error reading TIFF file {file_path}: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Data Source

# COMMAND ----------

# Register the custom data source
spark.dataSource.register(CopernicusTiffDataSource)

print("✅ Copernicus TIFF DataSource registered successfully!")
print("   Use format: spark.read.format('copernicus_tiff')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: Read a single TIFF file from Unity Catalog volume

# COMMAND ----------

# Example: Read Copernicus DEM data
# Adjust the path to match your actual volume location

# df = spark.read.format("copernicus_tiff") \
#     .option("h3_resolution", "8") \
#     .option("sampling_interval", "20") \
#     .option("max_rows", "1000") \
#     .load("/Volumes/demo_hc/climate_risk/terrain_data/copernicus_dem_sample.tif")
# 
# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Read and save to Delta table in Unity Catalog

# COMMAND ----------

# Example: Ingest TIFF data and save to Unity Catalog
# 
# tiff_path = "/Volumes/demo_hc/climate_risk/terrain_data/copernicus_dem.tif"
# 
# df = spark.read.format("copernicus_tiff") \
#     .option("h3_resolution", "8") \
#     .option("sampling_interval", "10") \
#     .load(tiff_path)
# 
# # Save to Unity Catalog
# df.write.format("delta") \
#     .mode("overwrite") \
#     .option("mergeSchema", "true") \
#     .saveAsTable("demo_hc.climate_risk.bronze_copernicus_dem")
# 
# print("✅ Data ingested to demo_hc.climate_risk.bronze_copernicus_dem")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Process multiple TIFF files

# COMMAND ----------

# Example: Process multiple TIFF files from a volume
# 
# import os
# 
# volume_path = "/Volumes/demo_hc/climate_risk/terrain_data/"
# tiff_files = [f for f in dbutils.fs.ls(volume_path) if f.name.endswith('.tif')]
# 
# for tiff_file in tiff_files:
#     print(f"Processing: {tiff_file.path}")
#     
#     df = spark.read.format("copernicus_tiff") \
#         .option("h3_resolution", "8") \
#         .option("sampling_interval", "15") \
#         .load(tiff_file.path)
#     
#     # Append to existing table
#     df.write.format("delta") \
#         .mode("append") \
#         .saveAsTable("demo_hc.climate_risk.bronze_copernicus_dem")
#     
#     print(f"  ✅ Completed: {tiff_file.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 4: Read with custom options

# COMMAND ----------

# Example: Read with all custom options
# 
# df = spark.read.format("copernicus_tiff") \
#     .option("h3_resolution", "9") \
#     .option("h3_resolution_regional", "7") \
#     .option("sampling_interval", "5") \
#     .option("extract_metadata", "true") \
#     .option("target_crs", "EPSG:4326") \
#     .option("max_rows", "5000") \
#     .load("/Volumes/demo_hc/climate_risk/terrain_data/copernicus_dem.tif")
# 
# # Show schema
# df.printSchema()
# 
# # Show statistics
# df.select("elevation_m").summary().show()
# 
# # Show sample data
# df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 5: Aggregate by H3 cell

# COMMAND ----------

# Example: Aggregate elevation data by H3 cell
# 
# df = spark.read.format("copernicus_tiff") \
#     .option("h3_resolution", "8") \
#     .option("sampling_interval", "10") \
#     .load("/Volumes/demo_hc/climate_risk/terrain_data/copernicus_dem.tif")
# 
# # Aggregate by H3 cell
# agg_df = df.groupBy("h3_cell_8", "h3_cell_6") \
#     .agg(
#         F.avg("elevation_m").alias("avg_elevation_m"),
#         F.min("elevation_m").alias("min_elevation_m"),
#         F.max("elevation_m").alias("max_elevation_m"),
#         F.stddev("elevation_m").alias("stddev_elevation_m"),
#         F.count("*").alias("pixel_count"),
#         F.first("longitude").alias("longitude"),
#         F.first("latitude").alias("latitude")
#     )
# 
# # Save aggregated data
# agg_df.write.format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("demo_hc.climate_risk.silver_terrain_h3_aggregated")
# 
# print("✅ Aggregated data saved to demo_hc.climate_risk.silver_terrain_h3_aggregated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions for Volume Management

# COMMAND ----------

def create_volume_if_not_exists(catalog: str, schema: str, volume: str):
    """
    Create a Unity Catalog volume if it doesn't exist.
    """
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
        print(f"✅ Volume {catalog}.{schema}.{volume} is ready")
    except Exception as e:
        print(f"⚠️ Error creating volume: {str(e)}")

def list_tiff_files_in_volume(volume_path: str) -> list:
    """
    List all TIFF files in a Unity Catalog volume.
    """
    try:
        files = dbutils.fs.ls(volume_path)
        tiff_files = [f for f in files if f.name.lower().endswith(('.tif', '.tiff'))]
        return tiff_files
    except Exception as e:
        print(f"Error listing files: {str(e)}")
        return []

def get_tiff_info(tiff_path: str) -> dict:
    """
    Get metadata information from a TIFF file without reading all data.
    """
    import rasterio
    
    with rasterio.open(tiff_path) as src:
        return {
            "width": src.width,
            "height": src.height,
            "crs": src.crs.to_string() if src.crs else None,
            "bounds": {
                "left": src.bounds.left,
                "bottom": src.bounds.bottom,
                "right": src.bounds.right,
                "top": src.bounds.top
            },
            "resolution": src.res,
            "nodata": src.nodata,
            "dtype": str(src.dtypes[0]),
            "count": src.count
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing and Validation

# COMMAND ----------

# Test the helper functions
# Uncomment to test

# # Create volume
# create_volume_if_not_exists("demo_hc", "climate_risk", "terrain_data")
# 
# # List TIFF files
# volume_path = "/Volumes/demo_hc/climate_risk/terrain_data/"
# tiff_files = list_tiff_files_in_volume(volume_path)
# print(f"Found {len(tiff_files)} TIFF files:")
# for f in tiff_files:
#     print(f"  - {f.name}")
# 
# # Get TIFF info
# if tiff_files:
#     info = get_tiff_info(tiff_files[0].path)
#     print(f"\nTIFF Info for {tiff_files[0].name}:")
#     print(json.dumps(info, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete Ingestion Pipeline Example

# COMMAND ----------

def ingest_copernicus_tiff_to_unity_catalog(
    source_path: str,
    target_table: str,
    h3_resolution: int = 8,
    sampling_interval: int = 10,
    mode: str = "overwrite"
):
    """
    Complete pipeline to ingest Copernicus TIFF data into Unity Catalog.
    
    Args:
        source_path: Path to TIFF file or directory in Unity Catalog volume
        target_table: Fully qualified Unity Catalog table name (catalog.schema.table)
        h3_resolution: H3 resolution for spatial indexing
        sampling_interval: Pixel sampling interval
        mode: Write mode ('overwrite', 'append', 'ignore', 'error')
    """
    print(f"🚀 Starting Copernicus TIFF ingestion pipeline")
    print(f"   Source: {source_path}")
    print(f"   Target: {target_table}")
    print(f"   H3 Resolution: {h3_resolution}")
    print(f"   Sampling Interval: {sampling_interval}")
    print("-" * 80)
    
    try:
        # Read TIFF data
        df = spark.read.format("copernicus_tiff") \
            .option("h3_resolution", str(h3_resolution)) \
            .option("sampling_interval", str(sampling_interval)) \
            .option("extract_metadata", "true") \
            .load(source_path)
        
        # Show preview
        print(f"\n📊 Data Preview:")
        df.show(5, truncate=False)
        
        # Show statistics
        print(f"\n📈 Elevation Statistics:")
        df.select("elevation_m").summary("count", "mean", "min", "max", "stddev").show()
        
        # Write to Unity Catalog
        print(f"\n💾 Writing to Unity Catalog table: {target_table}")
        df.write.format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true" if mode == "overwrite" else "false") \
            .saveAsTable(target_table)
        
        # Get row count
        row_count = spark.table(target_table).count()
        
        print(f"\n✅ Ingestion completed successfully!")
        print(f"   Total rows in table: {row_count:,}")
        print(f"   Table location: {target_table}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Ingestion failed: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Complete Pipeline
# MAGIC 
# MAGIC Uncomment and modify the following to run the complete ingestion pipeline:

# COMMAND ----------

# Example: Run the complete ingestion pipeline
# 
# result = ingest_copernicus_tiff_to_unity_catalog(
#     source_path="/Volumes/demo_hc/climate_risk/terrain_data/copernicus_dem.tif",
#     target_table="demo_hc.climate_risk.bronze_copernicus_dem",
#     h3_resolution=8,
#     sampling_interval=10,
#     mode="overwrite"
# )
# 
# if result:
#     # Query the ingested data
#     spark.sql("""
#         SELECT 
#             h3_cell_8,
#             COUNT(*) as pixel_count,
#             AVG(elevation_m) as avg_elevation,
#             MIN(elevation_m) as min_elevation,
#             MAX(elevation_m) as max_elevation
#         FROM demo_hc.climate_risk.bronze_copernicus_dem
#         GROUP BY h3_cell_8
#         ORDER BY avg_elevation DESC
#         LIMIT 20
#     """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This custom data source provides:
# MAGIC 
# MAGIC ✅ **TIFF File Reading**: Efficiently reads Copernicus DEM TIFF files  
# MAGIC ✅ **Spatial Indexing**: Automatic H3 spatial indexing at configurable resolutions  
# MAGIC ✅ **Metadata Extraction**: Extracts CRS, bounds, resolution, and other metadata  
# MAGIC ✅ **Flexible Sampling**: Configurable pixel sampling for performance optimization  
# MAGIC ✅ **Unity Catalog Integration**: Direct ingestion into Unity Catalog tables and volumes  
# MAGIC ✅ **Batch Processing**: Support for processing multiple TIFF files  
# MAGIC ✅ **CRS Transformation**: Automatic coordinate transformation to WGS84  
# MAGIC ✅ **Quality Control**: NoData value handling and validation  
# MAGIC 
# MAGIC ### Next Steps:
# MAGIC 
# MAGIC 1. Upload your Copernicus TIFF files to a Unity Catalog volume
# MAGIC 2. Uncomment and run the example cells above
# MAGIC 3. Monitor the ingestion process
# MAGIC 4. Query and analyze the ingested data
# MAGIC 5. Build downstream transformations and analytics

