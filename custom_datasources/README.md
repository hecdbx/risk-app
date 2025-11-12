# Copernicus TIFF Custom Data Source

A PySpark custom data source for ingesting Copernicus Digital Elevation Model (DEM) TIFF files into Databricks Unity Catalog.

## Features

✅ **TIFF File Reading**: Efficiently reads Copernicus DEM TIFF files using rasterio  
✅ **Spatial Indexing**: Automatic H3 spatial indexing at configurable resolutions  
✅ **Metadata Extraction**: Extracts CRS, bounds, resolution, and other metadata  
✅ **Flexible Sampling**: Configurable pixel sampling for performance optimization  
✅ **Unity Catalog Integration**: Direct ingestion into Unity Catalog tables and volumes  
✅ **Batch Processing**: Support for processing multiple TIFF files  
✅ **CRS Transformation**: Automatic coordinate transformation to WGS84  
✅ **Quality Control**: NoData value handling and validation  

## Requirements

- Databricks Runtime 15.4 LTS or above
- Python packages: `rasterio`, `h3`, `numpy`
- Unity Catalog enabled workspace

## Installation

1. Upload `copernicus_tiff_datasource.py` to your Databricks workspace
2. Run the notebook to register the data source
3. Start using it in your data pipelines

## Quick Start

### 1. Register the Data Source

```python
# Run the copernicus_tiff_datasource.py notebook
# The data source will be automatically registered
```

### 2. Read a TIFF File

```python
# Read from Unity Catalog volume
df = spark.read.format("copernicus_tiff") \
    .option("h3_resolution", "8") \
    .option("sampling_interval", "10") \
    .load("/Volumes/demo_hc/climate_risk/terrain_data/copernicus_dem.tif")

df.show()
```

### 3. Save to Unity Catalog

```python
# Save as Delta table
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("demo_hc.climate_risk.bronze_copernicus_dem")
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `path` | string | required | Path to TIFF file (Unity Catalog volume or cloud storage) |
| `h3_resolution` | int | 8 | H3 resolution level for spatial indexing |
| `h3_resolution_regional` | int | 6 | H3 resolution for regional aggregation |
| `sampling_interval` | int | 10 | Pixel sampling interval (1 = every pixel) |
| `extract_metadata` | boolean | true | Extract TIFF metadata (CRS, bounds, etc.) |
| `target_crs` | string | EPSG:4326 | Target CRS for coordinate transformation |
| `max_rows` | int | -1 | Maximum rows to read (-1 = unlimited) |

## Schema

The data source returns a DataFrame with the following schema:

```
root
 |-- file_path: string (nullable = false)
 |-- longitude: double (nullable = false)
 |-- latitude: double (nullable = false)
 |-- elevation_m: float (nullable = true)
 |-- h3_cell_8: string (nullable = true)
 |-- h3_cell_6: string (nullable = true)
 |-- pixel_x: integer (nullable = false)
 |-- pixel_y: integer (nullable = false)
 |-- crs: string (nullable = true)
 |-- nodata_value: float (nullable = true)
 |-- resolution_x: double (nullable = true)
 |-- resolution_y: double (nullable = true)
 |-- bounds_minx: double (nullable = true)
 |-- bounds_miny: double (nullable = true)
 |-- bounds_maxx: double (nullable = true)
 |-- bounds_maxy: double (nullable = true)
 |-- processing_timestamp: string (nullable = false)
```

## Usage Examples

### Example 1: Basic Ingestion

```python
# Read and display
df = spark.read.format("copernicus_tiff") \
    .option("sampling_interval", "20") \
    .load("/Volumes/demo_hc/climate_risk/terrain_data/dem.tif")

df.show(10)
```

### Example 2: High Resolution Processing

```python
# Process with higher H3 resolution
df = spark.read.format("copernicus_tiff") \
    .option("h3_resolution", "9") \
    .option("sampling_interval", "5") \
    .load("/Volumes/demo_hc/climate_risk/terrain_data/dem.tif")

# Aggregate by H3 cell
agg_df = df.groupBy("h3_cell_9").agg(
    F.avg("elevation_m").alias("avg_elevation"),
    F.stddev("elevation_m").alias("elevation_stddev"),
    F.count("*").alias("pixel_count")
)

agg_df.write.saveAsTable("demo_hc.climate_risk.terrain_h3_aggregated")
```

### Example 3: Batch Processing Multiple Files

```python
# Process all TIFF files in a volume
volume_path = "/Volumes/demo_hc/climate_risk/terrain_data/"
tiff_files = [f for f in dbutils.fs.ls(volume_path) if f.name.endswith('.tif')]

for tiff_file in tiff_files:
    print(f"Processing: {tiff_file.name}")
    
    df = spark.read.format("copernicus_tiff") \
        .option("sampling_interval", "15") \
        .load(tiff_file.path)
    
    df.write.format("delta") \
        .mode("append") \
        .saveAsTable("demo_hc.climate_risk.bronze_copernicus_dem")
```

### Example 4: Complete Pipeline Function

```python
def ingest_copernicus_tiff_to_unity_catalog(
    source_path: str,
    target_table: str,
    h3_resolution: int = 8,
    sampling_interval: int = 10,
    mode: str = "overwrite"
):
    """Ingest Copernicus TIFF data into Unity Catalog."""
    
    df = spark.read.format("copernicus_tiff") \
        .option("h3_resolution", str(h3_resolution)) \
        .option("sampling_interval", str(sampling_interval)) \
        .load(source_path)
    
    df.write.format("delta") \
        .mode(mode) \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
    
    return spark.table(target_table).count()

# Use the function
row_count = ingest_copernicus_tiff_to_unity_catalog(
    source_path="/Volumes/demo_hc/climate_risk/terrain_data/dem.tif",
    target_table="demo_hc.climate_risk.bronze_copernicus_dem"
)

print(f"✅ Ingested {row_count:,} rows")
```

## Performance Optimization

### Sampling Strategy

For large TIFF files, adjust the `sampling_interval` option:

- `sampling_interval=1`: Every pixel (highest resolution, slowest)
- `sampling_interval=10`: Every 10th pixel (good balance)
- `sampling_interval=20`: Every 20th pixel (faster, lower resolution)
- `sampling_interval=50`: Every 50th pixel (fastest, lowest resolution)

### H3 Resolution Guide

Choose H3 resolution based on your use case:

- **Resolution 6**: ~36 km² per cell - Regional analysis
- **Resolution 7**: ~5 km² per cell - Large area analysis
- **Resolution 8**: ~0.74 km² per cell - City-level analysis (recommended)
- **Resolution 9**: ~0.10 km² per cell - Neighborhood-level analysis
- **Resolution 10**: ~0.015 km² per cell - Building-level analysis

### Memory Considerations

For very large TIFF files:

1. Use higher `sampling_interval` values
2. Set `max_rows` to limit output
3. Process files in batches
4. Consider using `extract_metadata=false` for metadata-heavy operations

## Unity Catalog Volume Setup

### Create Volume

```sql
CREATE VOLUME IF NOT EXISTS demo_hc.climate_risk.terrain_data;
```

### Upload Files

```python
# Using dbutils
dbutils.fs.cp(
    "file:/tmp/copernicus_dem.tif",
    "/Volumes/demo_hc/climate_risk/terrain_data/copernicus_dem.tif"
)
```

## Troubleshooting

### Error: "UNSUPPORTED_FEATURE.PYTHON_DATA_SOURCE"

**Solution**: Upgrade to Databricks Runtime 15.4 LTS or above.

### Error: "Must specify a file path"

**Solution**: Provide path using `.load()` or `.option("path", "...")`:
```python
.load("/path/to/file.tif")
# or
.option("path", "/path/to/file.tif").load()
```

### Error: "No module named 'rasterio'"

**Solution**: Install required packages:
```python
%pip install rasterio h3 numpy
dbutils.library.restartPython()
```

### Performance Issues with Large Files

**Solution**: 
- Increase `sampling_interval`
- Set `max_rows` limit
- Use `extract_metadata=false`

## Data Sources

This data source supports:

- **Copernicus DEM**: Global 30m and 90m resolution
- **EU-DEM**: 25m resolution for Europe
- **Other TIFF/GeoTIFF**: Any standard elevation TIFF file

## Integration with Risk Analysis Pipeline

This custom data source integrates seamlessly with the existing climate risk pipeline:

```python
# Step 1: Ingest raw TIFF data
df = spark.read.format("copernicus_tiff") \
    .option("h3_resolution", "8") \
    .load("/Volumes/demo_hc/climate_risk/terrain_data/dem.tif")

# Step 2: Save to Bronze layer
df.write.saveAsTable("demo_hc.climate_risk.bronze_copernicus_dem")

# Step 3: Process through existing DLT pipeline
# The data flows through 01_terrain_dem_ingestion.py
# and into the flood/drought risk calculations
```

## License

Part of the European Climate Risk Pipeline project.

## Support

For issues or questions, contact the Climate Risk Analytics Team.

