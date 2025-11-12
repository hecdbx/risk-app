# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Paris Region TIFF Elevation Data
# MAGIC 
# MAGIC This notebook generates realistic synthetic TIFF elevation data for the Paris region.
# MAGIC The data can be used for testing the custom Copernicus TIFF data source.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Realistic elevation data for Paris (25m to 130m)
# MAGIC - Includes major landmarks: Montmartre, Belleville, Buttes-Chaumont
# MAGIC - Seine river valley
# MAGIC - Proper GeoTIFF format with CRS (EPSG:4326)
# MAGIC - Ready for Unity Catalog ingestion

# COMMAND ----------

# MAGIC %pip install rasterio numpy scipy --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import numpy as np
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS
from scipy.ndimage import gaussian_filter
from pathlib import Path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Output configuration
OUTPUT_PATH = "/tmp/paris_synthetic_dem.tif"
VOLUME_PATH = "/Volumes/demo_hc/climate_risk/terrain_data/paris_synthetic_dem.tif"

# DEM resolution
WIDTH = 2000   # pixels (higher = more detail)
HEIGHT = 2000  # pixels

# Paris bounding box (WGS84)
WEST, SOUTH = 2.224, 48.815   # Southwest corner
EAST, NORTH = 2.470, 48.902   # Northeast corner

print("📍 Paris Region DEM Configuration:")
print(f"   Resolution: {WIDTH}x{HEIGHT} pixels")
print(f"   Geographic bounds:")
print(f"     West:  {WEST:.4f}° E")
print(f"     East:  {EAST:.4f}° E")
print(f"     South: {SOUTH:.4f}° N")
print(f"     North: {NORTH:.4f}° N")
print(f"   Area: ~{(EAST-WEST)*111*(NORTH-SOUTH)*111:.1f} km²")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Synthetic Elevation Data
# MAGIC 
# MAGIC Create realistic elevation data with:
# MAGIC - Montmartre hill (~130m) in the north
# MAGIC - Belleville hill (~128m) in the northeast
# MAGIC - Buttes-Chaumont park
# MAGIC - Seine river valley (~25m)
# MAGIC - Natural terrain variation

# COMMAND ----------

print("🎨 Generating synthetic elevation data...")

# Create coordinate grids
x = np.linspace(WEST, EAST, WIDTH)
y = np.linspace(SOUTH, NORTH, HEIGHT)
X, Y = np.meshgrid(x, y)

# Base elevation (Paris city center average)
base_elevation = 42.0
elevation = np.ones((HEIGHT, WIDTH)) * base_elevation

# 1. Montmartre Hill (Sacré-Cœur)
# Location: 48.887°N, 2.340°E, Peak: ~130m
print("   Adding Montmartre hill...")
montmartre_lat, montmartre_lon = 48.887, 2.340
montmartre_dist = np.sqrt((X - montmartre_lon)**2 * 100 + (Y - montmartre_lat)**2 * 100)
montmartre_hill = 90 * np.exp(-montmartre_dist / 0.15)
elevation += montmartre_hill

# 2. Belleville Hill
# Location: 48.872°N, 2.390°E, Peak: ~128m
print("   Adding Belleville hill...")
belleville_lat, belleville_lon = 48.872, 2.390
belleville_dist = np.sqrt((X - belleville_lon)**2 * 100 + (Y - belleville_lat)**2 * 100)
belleville_hill = 85 * np.exp(-belleville_dist / 0.12)
elevation += belleville_hill

# 3. Buttes-Chaumont Park
# Location: 48.880°N, 2.383°E
print("   Adding Buttes-Chaumont...")
buttes_lat, buttes_lon = 48.880, 2.383
buttes_dist = np.sqrt((X - buttes_lon)**2 * 100 + (Y - buttes_lat)**2 * 100)
buttes_hill = 60 * np.exp(-buttes_dist / 0.10)
elevation += buttes_hill

# 4. Passy Hill (west)
# Location: 48.858°N, 2.285°E
print("   Adding Passy hill...")
passy_lat, passy_lon = 48.858, 2.285
passy_dist = np.sqrt((X - passy_lon)**2 * 100 + (Y - passy_lat)**2 * 100)
passy_hill = 40 * np.exp(-passy_dist / 0.12)
elevation += passy_hill

# 5. Seine River Valley
# River runs roughly SW to NE through Paris
print("   Creating Seine river valley...")
river_param = (X - WEST) / (EAST - WEST)
river_center_lat = SOUTH + river_param * (NORTH - SOUTH) - 0.01
river_dist = np.abs(Y - river_center_lat) * 100
river_valley = -20 * np.exp(-river_dist / 0.20)
elevation += river_valley

# 6. Natural terrain variation
print("   Adding natural terrain variation...")
np.random.seed(42)  # Reproducibility
natural_variation = np.random.normal(0, 2.5, (HEIGHT, WIDTH))
elevation += natural_variation

# 7. Smooth terrain
print("   Smoothing terrain...")
elevation = gaussian_filter(elevation, sigma=2.5)

# 8. Fine-scale features (buildings, roads, etc.)
print("   Adding fine-scale features...")
fine_variation = np.random.normal(0, 0.8, (HEIGHT, WIDTH))
fine_variation = gaussian_filter(fine_variation, sigma=0.5)
elevation += fine_variation

# Ensure realistic elevation bounds
elevation = np.clip(elevation, 25, 135)

# Convert to float32
elevation = elevation.astype(np.float32)

print("✅ Elevation data generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Elevation Statistics

# COMMAND ----------

print("📊 Elevation Statistics:")
print(f"   Min elevation:  {elevation.min():.2f} m")
print(f"   Max elevation:  {elevation.max():.2f} m")
print(f"   Mean elevation: {elevation.mean():.2f} m")
print(f"   Median:         {np.median(elevation):.2f} m")
print(f"   Std deviation:  {elevation.std():.2f} m")
print(f"\n   25th percentile: {np.percentile(elevation, 25):.2f} m")
print(f"   75th percentile: {np.percentile(elevation, 75):.2f} m")

# Elevation distribution
print(f"\n📈 Elevation Distribution:")
hist, bins = np.histogram(elevation, bins=10)
for i in range(len(hist)):
    bar = '█' * int(hist[i] / hist.max() * 50)
    print(f"   {bins[i]:5.1f}m - {bins[i+1]:5.1f}m: {bar} ({hist[i]:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create GeoTIFF File

# COMMAND ----------

print("💾 Creating GeoTIFF file...")

# Create transformation matrix
transform = from_bounds(WEST, SOUTH, EAST, NORTH, WIDTH, HEIGHT)

# Write GeoTIFF to local temp directory
with rasterio.open(
    OUTPUT_PATH,
    'w',
    driver='GTiff',
    height=HEIGHT,
    width=WIDTH,
    count=1,
    dtype=elevation.dtype,
    crs=CRS.from_epsg(4326),  # WGS84
    transform=transform,
    compress='lzw',
    tiled=True,
    blockxsize=256,
    blockysize=256,
    nodata=-9999
) as dst:
    dst.write(elevation, 1)
    dst.set_band_description(1, 'Elevation (meters above sea level)')
    
    # Add metadata
    dst.update_tags(
        AREA='Paris and Île-de-France region',
        SOURCE='Synthetic DEM for testing - realistic terrain features',
        VERTICAL_DATUM='EGM2008',
        DESCRIPTION='Realistic synthetic elevation data for Paris, France. Includes Montmartre, Belleville, Buttes-Chaumont, Seine valley.',
        GENERATOR='Databricks notebook - Paris TIFF generator',
        CREATED_DATE='2025-11-12',
        LANDMARKS='Montmartre (130m), Belleville (128m), Buttes-Chaumont, Passy, Seine River'
    )

print(f"✅ GeoTIFF created: {OUTPUT_PATH}")

# Get file size
file_size = Path(OUTPUT_PATH).stat().st_size
print(f"   File size: {file_size / 1024 / 1024:.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify TIFF File

# COMMAND ----------

print("🔍 Verifying TIFF file...")

with rasterio.open(OUTPUT_PATH) as src:
    print(f"\n📋 TIFF Metadata:")
    print(f"   Driver: {src.driver}")
    print(f"   Width: {src.width} pixels")
    print(f"   Height: {src.height} pixels")
    print(f"   Bands: {src.count}")
    print(f"   Data type: {src.dtypes[0]}")
    print(f"   CRS: {src.crs}")
    print(f"   Bounds: {src.bounds}")
    print(f"   Resolution: {src.res}")
    print(f"   NoData value: {src.nodata}")
    print(f"   Compression: {src.compression}")
    
    print(f"\n🏷️  Tags:")
    for key, value in src.tags().items():
        print(f"   {key}: {value}")
    
    # Read a sample of data
    sample = src.read(1, window=((500, 600), (500, 600)))
    print(f"\n📊 Sample data (100x100 pixels):")
    print(f"   Min: {sample.min():.2f} m")
    print(f"   Max: {sample.max():.2f} m")
    print(f"   Mean: {sample.mean():.2f} m")

print("\n✅ TIFF file verified successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload to Unity Catalog Volume

# COMMAND ----------

# Create volume if it doesn't exist
try:
    spark.sql("CREATE VOLUME IF NOT EXISTS demo_hc.climate_risk.terrain_data")
    print("✅ Volume demo_hc.climate_risk.terrain_data is ready")
except Exception as e:
    print(f"⚠️  Volume creation: {e}")

# COMMAND ----------

# Upload TIFF to volume
print(f"📤 Uploading to Unity Catalog volume...")
print(f"   Source: {OUTPUT_PATH}")
print(f"   Destination: {VOLUME_PATH}")

try:
    dbutils.fs.cp(f"file:{OUTPUT_PATH}", VOLUME_PATH, recurse=False)
    print(f"✅ File uploaded successfully!")
    
    # Verify upload
    file_info = dbutils.fs.ls("/Volumes/demo_hc/climate_risk/terrain_data/")
    print(f"\n📁 Files in volume:")
    for f in file_info:
        if 'paris' in f.name.lower():
            print(f"   ✓ {f.name} ({f.size / 1024 / 1024:.2f} MB)")
            
except Exception as e:
    print(f"❌ Upload failed: {e}")
    print(f"   Try manually: dbutils.fs.cp('file:{OUTPUT_PATH}', '{VOLUME_PATH}')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test with Custom Data Source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Custom Data Source (if not already registered)

# COMMAND ----------

# Uncomment if data source not registered yet
# %run ../custom_datasources/copernicus_tiff_datasource

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read TIFF using Custom Data Source

# COMMAND ----------

print("📖 Reading TIFF with custom data source...")

try:
    df = spark.read.format("copernicus_tiff") \
        .option("h3_resolution", "8") \
        .option("sampling_interval", "20") \
        .option("max_rows", "1000") \
        .load(VOLUME_PATH)
    
    print(f"✅ Data loaded successfully!")
    print(f"\n📊 Data preview:")
    df.show(10, truncate=False)
    
    print(f"\n📈 Elevation statistics from DataFrame:")
    df.select("elevation_m").summary().show()
    
    print(f"\n🗺️  H3 spatial distribution:")
    df.groupBy("h3_cell_8").count().orderBy("count", ascending=False).show(10)
    
except Exception as e:
    print(f"⚠️  Could not read with custom data source: {e}")
    print(f"   Make sure to run: %run ../custom_datasources/copernicus_tiff_datasource")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Elevation Data (Optional)

# COMMAND ----------

try:
    import matplotlib.pyplot as plt
    
    # Create visualization
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    # Elevation map
    im1 = axes[0].imshow(elevation, cmap='terrain', aspect='auto', 
                         extent=[WEST, EAST, SOUTH, NORTH])
    axes[0].set_title('Paris Region - Synthetic DEM', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Longitude (°E)')
    axes[0].set_ylabel('Latitude (°N)')
    cbar1 = plt.colorbar(im1, ax=axes[0], label='Elevation (m)')
    
    # Add landmark annotations
    axes[0].plot(2.340, 48.887, 'r*', markersize=15, label='Montmartre')
    axes[0].plot(2.390, 48.872, 'b*', markersize=15, label='Belleville')
    axes[0].plot(2.383, 48.880, 'g*', markersize=15, label='Buttes-Chaumont')
    axes[0].plot(2.3522, 48.8566, 'ko', markersize=10, label='Paris Center')
    axes[0].legend(loc='upper right')
    axes[0].grid(True, alpha=0.3)
    
    # Elevation histogram
    axes[1].hist(elevation.flatten(), bins=50, color='steelblue', edgecolor='black', alpha=0.7)
    axes[1].set_title('Elevation Distribution', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Elevation (m)')
    axes[1].set_ylabel('Frequency')
    axes[1].axvline(elevation.mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {elevation.mean():.1f}m')
    axes[1].axvline(np.median(elevation), color='green', linestyle='--', linewidth=2, label=f'Median: {np.median(elevation):.1f}m')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('/tmp/paris_dem_visualization.png', dpi=150, bbox_inches='tight')
    print("✅ Visualization saved to /tmp/paris_dem_visualization.png")
    
    display(plt.gcf())
    
except Exception as e:
    print(f"⚠️  Visualization skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("🎉 PARIS REGION TIFF GENERATION COMPLETE!")
print("=" * 80)
print(f"\n✅ Synthetic DEM created with realistic terrain features:")
print(f"   • Resolution: {WIDTH}x{HEIGHT} pixels")
print(f"   • Geographic area: Paris and surrounding region")
print(f"   • Elevation range: 25m - 135m")
print(f"   • Features: Montmartre, Belleville, Buttes-Chaumont, Seine valley")
print(f"   • File size: {file_size / 1024 / 1024:.2f} MB")
print(f"\n📍 Location: {VOLUME_PATH}")
print(f"\n🔧 Next steps:")
print(f"   1. Use custom data source to ingest:")
print(f"      spark.read.format('copernicus_tiff').load('{VOLUME_PATH}')")
print(f"   2. Save to Unity Catalog:")
print(f"      df.write.saveAsTable('demo_hc.climate_risk.bronze_copernicus_paris')")
print(f"   3. Process through DLT pipelines")
print(f"   4. Analyze flood/drought risks for Paris")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Paris TIFF Files
# MAGIC 
# MAGIC You can create multiple TIFF files for different areas or resolutions:

# COMMAND ----------

# Example: Create higher resolution TIFF for central Paris only
# Uncomment to generate

# CENTRAL_PARIS_WEST, CENTRAL_PARIS_SOUTH = 2.28, 48.83
# CENTRAL_PARIS_EAST, CENTRAL_PARIS_NORTH = 2.40, 48.88
# 
# # Generate with higher resolution
# # (code similar to above but with tighter bounds and higher resolution)

