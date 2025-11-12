"""
Generate TIFF elevation data for Paris region.

This script can:
1. Download real Copernicus DEM data for Paris (requires internet)
2. Generate synthetic but realistic elevation data for testing

Usage:
    python generate_paris_tiff.py --mode real --output paris_dem.tif
    python generate_paris_tiff.py --mode synthetic --output paris_synthetic_dem.tif
"""

import numpy as np
import argparse
from pathlib import Path

def generate_synthetic_paris_dem(output_path: str, width: int = 1000, height: int = 1000):
    """
    Generate synthetic but realistic elevation data for Paris region.
    
    Paris elevation ranges from ~25m to ~130m, with the Seine river running through it.
    We'll create a realistic terrain with:
    - Base elevation around 35-50m (city center)
    - Hills in the north (Montmartre ~130m)
    - River valley (Seine ~25m)
    - Gradual elevation changes
    """
    try:
        import rasterio
        from rasterio.transform import from_bounds
        from rasterio.crs import CRS
        from scipy.ndimage import gaussian_filter
    except ImportError:
        print("❌ Missing required packages. Install with:")
        print("   pip install rasterio numpy scipy")
        return False
    
    print(f"🎨 Generating synthetic DEM for Paris region...")
    print(f"   Resolution: {width}x{height} pixels")
    
    # Paris bounding box (approximate)
    # Lon: 2.224 to 2.470 (East-West)
    # Lat: 48.815 to 48.902 (South-North)
    west, south = 2.224, 48.815
    east, north = 2.470, 48.902
    
    # Create coordinate grids
    x = np.linspace(west, east, width)
    y = np.linspace(south, north, height)
    X, Y = np.meshgrid(x, y)
    
    # Base elevation (35-50m average for Paris)
    base_elevation = 42.0
    
    # Create elevation features
    elevation = np.ones((height, width)) * base_elevation
    
    # 1. Add Montmartre hill in the north (around 18th arrondissement)
    # Montmartre peak: ~48.887°N, 2.340°E, elevation ~130m
    montmartre_lat, montmartre_lon = 48.887, 2.340
    montmartre_dist = np.sqrt((X - montmartre_lon)**2 * 100 + (Y - montmartre_lat)**2 * 100)
    montmartre_hill = 90 * np.exp(-montmartre_dist / 0.15)  # Peak adds ~90m
    elevation += montmartre_hill
    
    # 2. Add Belleville hill in the northeast
    # Belleville: ~48.872°N, 2.390°E, elevation ~128m
    belleville_lat, belleville_lon = 48.872, 2.390
    belleville_dist = np.sqrt((X - belleville_lon)**2 * 100 + (Y - belleville_lat)**2 * 100)
    belleville_hill = 85 * np.exp(-belleville_dist / 0.12)  # Peak adds ~85m
    elevation += belleville_hill
    
    # 3. Add Buttes-Chaumont area
    # Buttes-Chaumont: ~48.880°N, 2.383°E
    buttes_lat, buttes_lon = 48.880, 2.383
    buttes_dist = np.sqrt((X - buttes_lon)**2 * 100 + (Y - buttes_lat)**2 * 100)
    buttes_hill = 60 * np.exp(-buttes_dist / 0.10)
    elevation += buttes_hill
    
    # 4. Create Seine river valley (running roughly SW to NE through Paris)
    # Seine runs approximately through: (2.25, 48.85) to (2.42, 48.89)
    # River centerline
    river_param = (X - 2.25) / (2.42 - 2.25)
    river_center_lat = 48.85 + river_param * (48.89 - 48.85)
    river_dist = np.abs(Y - river_center_lat) * 100
    
    # River valley effect (lowers elevation near river)
    river_valley = -20 * np.exp(-river_dist / 0.20)
    elevation += river_valley
    
    # 5. Add some random natural variation
    np.random.seed(42)  # For reproducibility
    natural_variation = np.random.normal(0, 2, (height, width))
    elevation += natural_variation
    
    # 6. Smooth the elevation to make it more realistic
    elevation = gaussian_filter(elevation, sigma=2.0)
    
    # 7. Add fine-scale terrain features
    fine_variation = np.random.normal(0, 0.5, (height, width))
    fine_variation = gaussian_filter(fine_variation, sigma=0.5)
    elevation += fine_variation
    
    # Ensure realistic bounds (Paris elevation: 25m to 130m)
    elevation = np.clip(elevation, 25, 130)
    
    # Convert to float32 for TIFF
    elevation = elevation.astype(np.float32)
    
    # Create transformation matrix
    transform = from_bounds(west, south, east, north, width, height)
    
    # Write GeoTIFF
    print(f"💾 Writing GeoTIFF to: {output_path}")
    
    with rasterio.open(
        output_path,
        'w',
        driver='GTiff',
        height=height,
        width=width,
        count=1,
        dtype=elevation.dtype,
        crs=CRS.from_epsg(4326),  # WGS84
        transform=transform,
        compress='lzw',
        nodata=-9999
    ) as dst:
        dst.write(elevation, 1)
        dst.set_band_description(1, 'Elevation (meters above sea level)')
        
        # Add metadata
        dst.update_tags(
            AREA='Paris and surrounding region',
            SOURCE='Synthetic DEM for testing',
            VERTICAL_DATUM='EGM2008',
            DESCRIPTION='Realistic synthetic elevation data for Paris, France'
        )
    
    # Print statistics
    print(f"\n📊 DEM Statistics:")
    print(f"   Min elevation: {elevation.min():.2f} m")
    print(f"   Max elevation: {elevation.max():.2f} m")
    print(f"   Mean elevation: {elevation.mean():.2f} m")
    print(f"   Std deviation: {elevation.std():.2f} m")
    print(f"   Geographic bounds:")
    print(f"     West:  {west:.4f}°")
    print(f"     East:  {east:.4f}°")
    print(f"     South: {south:.4f}°")
    print(f"     North: {north:.4f}°")
    print(f"\n✅ Synthetic DEM created successfully!")
    
    return True


def download_real_copernicus_dem(output_path: str, region: str = 'paris'):
    """
    Download real Copernicus DEM data for Paris region.
    
    Note: This requires internet connection and access to Copernicus data services.
    """
    try:
        import requests
        import rasterio
        from rasterio.merge import merge
        from rasterio.mask import mask
        from shapely.geometry import box
    except ImportError:
        print("❌ Missing required packages. Install with:")
        print("   pip install rasterio requests shapely")
        return False
    
    print(f"🌍 Attempting to download real Copernicus DEM data for {region}...")
    print(f"⚠️  Note: This requires Copernicus credentials and API access")
    
    # Paris bounding box
    west, south = 2.224, 48.815
    east, north = 2.470, 48.902
    
    # Copernicus DEM is available through various services:
    # 1. OpenTopography
    # 2. AWS S3 (public dataset)
    # 3. Copernicus Data Space Ecosystem
    
    print(f"\n📍 Region coordinates:")
    print(f"   West:  {west:.4f}°")
    print(f"   East:  {east:.4f}°")
    print(f"   South: {south:.4f}°")
    print(f"   North: {north:.4f}°")
    
    print(f"\n💡 To download real Copernicus DEM data:")
    print(f"\n1. Using OpenTopography:")
    print(f"   - Visit: https://portal.opentopography.org/")
    print(f"   - Select: Copernicus GLO-30 DEM")
    print(f"   - Bounds: {west:.4f}, {south:.4f}, {east:.4f}, {north:.4f}")
    print(f"   - Format: GeoTIFF")
    
    print(f"\n2. Using AWS S3 (Copernicus DEM GLO-30):")
    print(f"   aws s3 cp s3://copernicus-dem-30m/ . --recursive --no-sign-request")
    print(f"   (Look for tiles covering Paris: N48E002)")
    
    print(f"\n3. Using Copernicus Data Space:")
    print(f"   - Visit: https://dataspace.copernicus.eu/")
    print(f"   - Create account (free)")
    print(f"   - Search for: Copernicus DEM")
    print(f"   - Filter by Paris region")
    
    print(f"\n⚠️  For now, use synthetic data or manually download the DEM.")
    print(f"   Then use: python generate_paris_tiff.py --mode process --input <downloaded_file>")
    
    return False


def process_existing_dem(input_path: str, output_path: str, bounds: tuple = None):
    """
    Process an existing DEM file (crop, reproject, etc.)
    """
    try:
        import rasterio
        from rasterio.mask import mask
        from rasterio.warp import calculate_default_transform, reproject, Resampling
        from shapely.geometry import box
    except ImportError:
        print("❌ Missing required packages. Install with:")
        print("   pip install rasterio shapely")
        return False
    
    print(f"🔄 Processing existing DEM: {input_path}")
    
    # Default to Paris bounds if not specified
    if bounds is None:
        bounds = (2.224, 48.815, 2.470, 48.902)  # west, south, east, north
    
    west, south, east, north = bounds
    
    with rasterio.open(input_path) as src:
        print(f"   Input CRS: {src.crs}")
        print(f"   Input bounds: {src.bounds}")
        print(f"   Input size: {src.width}x{src.height}")
        
        # Create bounding box for Paris
        bbox = box(west, south, east, north)
        
        # Crop to Paris region
        out_image, out_transform = mask(src, [bbox], crop=True)
        out_meta = src.meta.copy()
        
        out_meta.update({
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
            "compress": "lzw"
        })
        
        # Write cropped DEM
        with rasterio.open(output_path, "w", **out_meta) as dest:
            dest.write(out_image)
        
        print(f"✅ Processed DEM saved to: {output_path}")
        print(f"   Output size: {out_image.shape[2]}x{out_image.shape[1]}")
        
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Generate or download TIFF elevation data for Paris region'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['synthetic', 'real', 'process'],
        default='synthetic',
        help='Mode: synthetic (generate), real (download), or process (crop existing)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='paris_dem.tif',
        help='Output TIFF file path'
    )
    parser.add_argument(
        '--input',
        type=str,
        help='Input TIFF file path (for process mode)'
    )
    parser.add_argument(
        '--width',
        type=int,
        default=1000,
        help='Width in pixels (for synthetic mode)'
    )
    parser.add_argument(
        '--height',
        type=int,
        default=1000,
        help='Height in pixels (for synthetic mode)'
    )
    
    args = parser.parse_args()
    
    # Create output directory if needed
    output_dir = Path(args.output).parent
    if output_dir != Path('.'):
        output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("🗼 Paris Region DEM Generator")
    print("=" * 80)
    
    if args.mode == 'synthetic':
        success = generate_synthetic_paris_dem(args.output, args.width, args.height)
    elif args.mode == 'real':
        success = download_real_copernicus_dem(args.output)
    elif args.mode == 'process':
        if not args.input:
            print("❌ Error: --input required for process mode")
            return 1
        success = process_existing_dem(args.input, args.output)
    
    if success:
        print(f"\n🎉 Success! TIFF file created: {args.output}")
        print(f"\n📝 Next steps:")
        print(f"   1. Upload to Databricks volume:")
        print(f"      dbutils.fs.cp('file:/tmp/{Path(args.output).name}', '/Volumes/demo_hc/climate_risk/terrain_data/{Path(args.output).name}')")
        print(f"   2. Use custom data source:")
        print(f"      spark.read.format('copernicus_tiff').load('/Volumes/demo_hc/climate_risk/terrain_data/{Path(args.output).name}')")
        return 0
    else:
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())

