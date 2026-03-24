import ee

def initialize_gee(project_id):
    try:
        ee.Initialize(project=project_id)
    except Exception:
        ee.Authenticate()
        ee.Initialize(project=project_id)

def mask_s2_clouds(image):
    scl = image.select("SCL")

    mask = (
        scl.neq(3)         # cloud shadow
        .And(scl.neq(8))   # cloud medium probability
        .And(scl.neq(9))   # cloud high probability
        .And(scl.neq(10))  # thin cirrus
    )
    return image.updateMask(mask)


def add_spectral_indices(image):
    
    ndsi = image.normalizedDifference(["B3", "B11"]).rename("NDSI") # Normalized Difference Snow Index
    ndvi = image.normalizedDifference(["B8", "B4"]).rename("NDVI") # Normalized Difference Vegetation Index

    return image.addBands([ndsi, ndvi])


def preprocess_s2_image(image, polygon):
    original = image

    optical_bands = image.select(["B2", "B3", "B4", "B8", "B11", "B12"]).divide(10000) 
    scl = image.select("SCL")
    
    processed = optical_bands.addBands(scl)
    processed = mask_s2_clouds(processed)
    processed = add_spectral_indices(processed)
    processed = processed.clip(polygon)

    return processed.copyProperties(original, original.propertyNames())


def get_glacier_collection(polygon, observation_start, observation_end, cloud_threshold=40):

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(polygon)
        .filterDate(observation_start, observation_end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", cloud_threshold))
        .map(lambda img: preprocess_s2_image(img, polygon))
    )
    return collection


def get_dem(polygon):
    
    dem = (
        ee.ImageCollection("COPERNICUS/DEM/GLO30")
        .select("DEM")
        .mosaic()
        .clip(polygon)
    )

    slope = ee.Terrain.slope(dem).rename("slope")
    aspect = ee.Terrain.aspect(dem).rename("aspect")

    return dem.rename("DEM").addBands([slope, aspect])