"""Create map tiles at a specified zoom level covering a geographic area.

This script generates tile boundaries based on the Mercantile library for map
tiling at a given zoom level. It can either cover the entire world or be
restricted to a specific polygon (e.g., a country boundary). The output is saved
as a GeoPackage file containing tile geometries and coordinates.

Configuration paths and parameters are loaded from a config file via start.py.
"""

import os
import logging
import mercantile
import geopandas as gpd
from shapely.geometry import box
from start import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_tiles_from_polygon(polygon=None, zoom_level=8):
    """Generate a GeoDataFrame of tiles covering a polygon at a specified zoom level.
    
    Args:
        polygon: A Shapely geometry object representing the area to cover. If None,
                 defaults to covering the entire world (-180 to 180 lon, -90 to 90 lat).
        zoom_level: Integer representing the zoom level for tile generation (default: 8).
    
    Returns:
        A GeoDataFrame with columns:
            - geometry: Polygon geometries of tile boundaries
            - id: Unique tile identifier
            - x, y, z: Tile coordinates (x, y at given zoom level z)
    """
    logging.info(f"Generating tiles at zoom level {zoom_level}")
    
    if polygon is None:
        polygon = box(-180, -90, 180, 90)
        logging.debug("No polygon provided, using world bounds")
    
    tiles = list(mercantile.tiles(*polygon.bounds, zoom_level))
    logging.info(f"Generated {len(tiles)} tiles")
    
    geometries = []
    for tile in tiles:
        tile_bbox = mercantile.bounds(tile)
        geometries.append(box(tile_bbox.west, tile_bbox.south, tile_bbox.east, tile_bbox.north))
    
    gdf_tiles = gpd.GeoDataFrame(geometry=geometries, crs='EPSG:4326')
    gdf_tiles['id'] = range(len(tiles))
    gdf_tiles['x'] = [tile.x for tile in tiles]
    gdf_tiles['y'] = [tile.y for tile in tiles]
    gdf_tiles['z'] = [tile.z for tile in tiles]
    
    logging.debug(f"Created GeoDataFrame with {len(gdf_tiles)} tile records")
    return gdf_tiles

if __name__ == '__main__':
    logging.info("Starting tile creation script")
    
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    logging.debug(f"Working directory: {os.getcwd()}")
    
    cfg = load_config()
    logging.info("Configuration loaded successfully")

    zoom_level = cfg['params']['zoom_level']
    save_dir = cfg['paths']['tiles_save_dir']
    polygon_filename = cfg['filenames']['starter_polygon_fn']
    logging.debug(f"Zoom level: {zoom_level}, Save directory: {save_dir}")

    polygon_filename = polygon_filename if polygon_filename != '' else None
    polygon_filepath = os.path.join(cfg['paths']['starter_dir'], polygon_filename) if polygon_filename else None

    # Try to load country-specific polygon
    polygon = None
    try:
        polygon_filename = cfg['filenames']['country_filename']
        polygon_filepath = os.path.join(cfg['paths']['starter_dir'], polygon_filename) if polygon_filename else None
        logging.info(f"Attempting to load polygon from {polygon_filepath}")
        polygon = gpd.read_parquet(polygon_filepath)
        polygon = polygon[polygon['country'] == 'MG'].geometry.values[0]
        logging.info("Successfully loaded Madagascar polygon")
    except Exception as e:
        logging.warning(f"Could not load polygon: {e}. Will generate tiles for entire world.")
        polygon = None

    # Create output directory if it doesn't exist
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
        logging.info(f"Created output directory: {save_dir}")
    
    # Generate tiles
    logging.info("Generating tiles from polygon")
    tiles_gdf = get_tiles_from_polygon(polygon=polygon, zoom_level=zoom_level)
    
    # Save to file
    output_filepath = os.path.join(save_dir, f'tiles_z{zoom_level}.gpkg')
    logging.info(f"Saving {len(tiles_gdf)} tiles to {output_filepath}")
    tiles_gdf.to_file(output_filepath, driver='GPKG', index=False)
    
    logging.info(f'Successfully saved tiles to {output_filepath}')
    print(f'Saved tiles to {output_filepath}')



