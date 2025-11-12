# Paris Region TIFF Generator Tools

Tools for generating realistic TIFF elevation data for the Paris region to use with the custom Copernicus data source.

## Files

### 1. `generate_paris_tiff.py` - Command-line Script

Python script that can run locally or in any Python environment.

**Features:**
- Generate synthetic realistic DEM for Paris
- Download real Copernicus data (with instructions)
- Process existing TIFF files

**Usage:**

```bash
# Generate synthetic TIFF (default)
python generate_paris_tiff.py --mode synthetic --output paris_dem.tif

# With custom resolution
python generate_paris_tiff.py --mode synthetic --width 2000 --height 2000 --output paris_high_res.tif

# Process existing TIFF
python generate_paris_tiff.py --mode process --input copernicus_raw.tif --output paris_cropped.tif

# Get instructions for downloading real data
python generate_paris_tiff.py --mode real
```

**Requirements:**
```bash
pip install rasterio numpy scipy shapely
```

### 2. `generate_paris_tiff_notebook.py` - Databricks Notebook

Complete Databricks notebook for generating Paris TIFF data directly in your workspace.

**Features:**
- Interactive generation with visualizations
- Automatic upload to Unity Catalog volume
- Integration with custom data source
- Data quality validation
- Matplotlib visualizations

**Usage:**
1. Upload notebook to Databricks
2. Run all cells
3. TIFF file will be created and uploaded to `/Volumes/demo_hc/climate_risk/terrain_data/`

## Generated Data Specifications

### Geographic Coverage

- **Region:** Paris and Île-de-France
- **Bounding Box:**
  - West: 2.224°E
  - East: 2.470°E
  - South: 48.815°N
  - North: 48.902°N
- **Area:** ~27 km × 10 km

### Elevation Features

The synthetic DEM includes realistic terrain features:

| Feature | Location | Elevation | Description |
|---------|----------|-----------|-------------|
| **Montmartre** | 48.887°N, 2.340°E | ~130m | Highest point in Paris, Sacré-Cœur |
| **Belleville** | 48.872°N, 2.390°E | ~128m | Second highest hill |
| **Buttes-Chaumont** | 48.880°N, 2.383°E | ~100m | Park with elevation |
| **Passy** | 48.858°N, 2.285°E | ~70m | Western hills |
| **Seine Valley** | Through Paris | ~25m | River valley (lowest) |
| **City Average** | - | ~42m | General city elevation |

### Technical Specifications

- **Format:** GeoTIFF
- **CRS:** EPSG:4326 (WGS84)
- **Data Type:** Float32
- **Compression:** LZW
- **NoData Value:** -9999
- **Tile Size:** 256x256
- **Default Resolution:** 1000x1000 pixels (adjustable)

### Elevation Statistics

- **Min:** 25m (Seine river)
- **Max:** 130-135m (Montmartre)
- **Mean:** ~45m
- **Std Dev:** ~15m
- **Range:** 110m

## Use Cases

### 1. Testing Custom Data Source

```python
# Generate test data
python generate_paris_tiff.py --output test_paris.tif

# Use with custom data source
spark.read.format("copernicus_tiff") \
    .option("h3_resolution", "8") \
    .load("/Volumes/demo_hc/climate_risk/terrain_data/test_paris.tif")
```

### 2. Development and CI/CD

Use synthetic data for:
- Unit testing
- Integration testing
- CI/CD pipelines
- Demo and training

### 3. Prototype Development

Develop pipelines without downloading large real datasets:
- Test terrain analysis algorithms
- Validate H3 indexing
- Test flood risk calculations
- Develop visualizations

### 4. Performance Testing

Generate different resolutions for performance testing:

```bash
# Small (fast)
python generate_paris_tiff.py --width 500 --height 500 --output paris_small.tif

# Medium (balanced)
python generate_paris_tiff.py --width 1000 --height 1000 --output paris_medium.tif

# Large (high detail)
python generate_paris_tiff.py --width 3000 --height 3000 --output paris_large.tif
```

## Getting Real Copernicus Data

For production use, download real Copernicus DEM data:

### Option 1: OpenTopography

1. Visit: https://portal.opentopography.org/
2. Select: **Copernicus GLO-30 DEM**
3. Set bounds: `2.224, 48.815, 2.470, 48.902`
4. Format: GeoTIFF
5. Download

### Option 2: AWS S3 (Public Dataset)

```bash
# Install AWS CLI
pip install awscli

# Download Copernicus DEM tiles for Paris
aws s3 cp s3://copernicus-dem-30m/Copernicus_DSM_COG_10_N48_00_E002_00_DEM/ . --recursive --no-sign-request

# Paris is in tile: N48E002
```

### Option 3: Copernicus Data Space

1. Visit: https://dataspace.copernicus.eu/
2. Create free account
3. Search: "Copernicus DEM"
4. Filter by Paris region (48.8°N, 2.3°E)
5. Download

### Option 4: Process Downloaded Data

```bash
# Crop downloaded tile to Paris region
python generate_paris_tiff.py --mode process \
    --input Copernicus_DSM_10_N48_00_E002_00_DEM.tif \
    --output paris_copernicus_real.tif
```

## Integration Examples

### Example 1: Basic Pipeline

```python
# Generate data
!python tools/generate_paris_tiff.py --output paris.tif

# Upload to volume
dbutils.fs.cp("file:/tmp/paris.tif", "/Volumes/demo_hc/climate_risk/terrain_data/paris.tif")

# Ingest with custom data source
df = spark.read.format("copernicus_tiff").load("/Volumes/demo_hc/climate_risk/terrain_data/paris.tif")

# Save to Unity Catalog
df.write.saveAsTable("demo_hc.climate_risk.bronze_copernicus_paris")
```

### Example 2: Multiple Resolutions

```python
# Generate multiple resolutions for different use cases
resolutions = {
    "overview": (500, 500),
    "standard": (1000, 1000),
    "detailed": (2000, 2000)
}

for name, (width, height) in resolutions.items():
    output = f"paris_{name}.tif"
    !python tools/generate_paris_tiff.py --width {width} --height {height} --output {output}
```

### Example 3: Automated Testing

```python
import unittest

class TestTerrainPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Generate test data
        !python tools/generate_paris_tiff.py --width 100 --height 100 --output test_paris.tif
        
        # Load test data
        cls.df = spark.read.format("copernicus_tiff").load("test_paris.tif")
    
    def test_elevation_range(self):
        stats = self.df.select("elevation_m").summary().collect()
        min_elev = float(stats[3]["elevation_m"])
        max_elev = float(stats[7]["elevation_m"])
        
        self.assertGreaterEqual(min_elev, 20)
        self.assertLessEqual(max_elev, 140)
```

## Visualization

The notebook includes matplotlib visualizations:
- Elevation map with landmark annotations
- Histogram of elevation distribution
- Statistical summaries

## File Sizes

Approximate file sizes (with LZW compression):

| Resolution | Uncompressed | Compressed | Load Time* |
|------------|--------------|------------|------------|
| 500x500 | ~1 MB | ~0.3 MB | <1s |
| 1000x1000 | ~4 MB | ~1 MB | <2s |
| 2000x2000 | ~16 MB | ~4 MB | ~5s |
| 3000x3000 | ~36 MB | ~9 MB | ~10s |

*Approximate Spark read time with sampling_interval=10

## Troubleshooting

### Issue: "ModuleNotFoundError: No module named 'rasterio'"

**Solution:**
```bash
pip install rasterio numpy scipy
```

### Issue: "Permission denied" when writing file

**Solution:**
```bash
# Use /tmp directory
python generate_paris_tiff.py --output /tmp/paris.tif
```

### Issue: Generated file is too large

**Solution:**
```bash
# Reduce resolution
python generate_paris_tiff.py --width 500 --height 500 --output paris_small.tif
```

### Issue: Elevation values seem unrealistic

**Solution:**
The synthetic data includes realistic Paris elevations (25-135m). If you need different ranges, modify the script or use real Copernicus data.

## Contributing

To add new terrain features:
1. Edit `generate_paris_tiff.py` or `generate_paris_tiff_notebook.py`
2. Add new elevation features using the same Gaussian hill pattern
3. Test with different resolutions
4. Update documentation

## License

Part of the European Climate Risk Pipeline project.

## Support

For issues or questions:
- Check the main project README
- Review custom data source documentation
- Contact: Climate Risk Analytics Team

