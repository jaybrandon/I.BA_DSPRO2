import ee


def initialize_gee(project_id):
    try:
        ee.Initialize(project=project_id)
    except Exception:
        ee.Authenticate()
        ee.Initialize(project=project_id)


SAT_CONFIG = {
    "sentinel2": {
        "id": "COPERNICUS/S2_SR_HARMONIZED",
        "bands": ["B2", "B3", "B4", "B8", "B11", "B12"],
        "scale": 1 / 10000.0,
        "offset": 0,
        "cloud_prop": "CLOUDY_PIXEL_PERCENTAGE",
    },
    "landsat8": {
        "id": "LANDSAT/LC08/C02/T1_L2",
        "bands_src": ["SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7"],
        "scale": 0.0000275,
        "offset": -0.2,
        "cloud_prop": "CLOUD_COVER",
    },
    "landsat5": {
        "id": "LANDSAT/LT05/C02/T1_L2",
        "bands_src": ["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B7"],
        "scale": 0.0000275,
        "offset": -0.2,
        "cloud_prop": "CLOUD_COVER",
    },
}


def mask_clouds(image, sensor_type):
    if sensor_type == "sentinel2":
        scl = image.select("SCL")
        mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    else:
        qa = image.select("QA_PIXEL")
        # 3=cloud, 4=cloud Shadow, 2=cirrus
        mask = qa.bitwiseAnd(1 << 3).eq(0).And(qa.bitwiseAnd(1 << 4).eq(0))
    return image.updateMask(mask)


def preprocess_image(image, sensor_type, polygon):
    config = SAT_CONFIG[sensor_type]

    # Select correct Bands
    src_bands = config.get("bands_src", config.get("bands"))
    dst_bands = ["B2", "B3", "B4", "B8", "B11", "B12"]

    optical = (
        image.select(src_bands, dst_bands)
        .multiply(config["scale"])
        .add(config["offset"])
    )

    # mask bands (SCL -> S2, QA -> Landsat)
    mask_band = "SCL" if sensor_type == "sentinel2" else "QA_PIXEL"
    processed = optical.addBands(image.select(mask_band))

    # apply masking and indices
    processed = mask_clouds(processed, sensor_type)

    # NDSI (Green, SWIR1) -> B3, B11 across all
    ndsi = processed.normalizedDifference(["B3", "B11"]).rename("NDSI")
    ndvi = processed.normalizedDifference(["B8", "B4"]).rename("NDVI")

    return (
        processed.addBands([ndsi, ndvi])
        .clip(polygon)
        .copyProperties(image, ["system:time_start"])
    )


def get_glacier_collection(
    sensor_type, polygon, start_date, end_date, cloud_threshold=40
):
    config = SAT_CONFIG[sensor_type]

    collection = (
        ee.ImageCollection(config["id"])
        .filterBounds(polygon)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.calendarRange(8, 9, "month"))
        .filter(ee.Filter.lt(config["cloud_prop"], cloud_threshold))
        .map(lambda img: preprocess_image(img, sensor_type, polygon))
    )
    return collection


def get_dem(roi: ee.Geometry) -> ee.Image:
    dem_collection = ee.ImageCollection("COPERNICUS/DEM/GLO30")
    dem_image = dem_collection.select("DEM").mosaic()
    terrain = ee.Terrain.products(dem_image)
    return terrain.clip(roi)
