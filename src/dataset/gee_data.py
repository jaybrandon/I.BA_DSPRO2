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
        mask = (
            qa.bitwiseAnd(1 << 1)
            .eq(0)
            .And(qa.bitwiseAnd(1 << 2).eq(0))
            .And(qa.bitwiseAnd(1 << 3).eq(0))
            .And(qa.bitwiseAnd(1 << 4).eq(0))
        )
    return image.updateMask(mask)


def preprocess_image(image, sensor_type):
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

    return processed.copyProperties(image, ["system:time_start"])



def mosaic_by_date(collection, polygon):
    def add_date(img):
        date_str = img.date().format("YYYY-MM-DD")
        return img.set("date", date_str)

    col_with_date = collection.map(add_date)

    unique_dates = col_with_date.aggregate_array("date").distinct()

    def mosaic_for_date(date_str):
        date_str = ee.String(date_str)

        daily_col = col_with_date.filter(ee.Filter.eq("date", date_str))

        mosaicked = daily_col.mosaic().clip(polygon)

        first_img = daily_col.first()
        return mosaicked.copyProperties(first_img, ["system:time_start", "date"])

    return ee.ImageCollection(unique_dates.map(mosaic_for_date))


def get_glacier_composite(
    sensor_type, polygon, start_date, end_date, cloud_threshold=40
):
    config = SAT_CONFIG[sensor_type]

    collection = (
        ee.ImageCollection(config["id"])
        .filterBounds(polygon)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.calendarRange(7, 9, "month"))
        .filter(ee.Filter.lt(str(config["cloud_prop"]), cloud_threshold))
        .map(lambda img: preprocess_image(img, sensor_type))
    )

    if collection.size().getInfo() == 0:
        return None

    median_image = collection.median().clip(polygon)

    valid_mask = median_image.select("B2").mask().unmask(0)

    pixel_coverage = (
        valid_mask.reduceRegion(
            reducer=ee.Reducer.mean(), geometry=polygon, scale=30, maxPixels=1e9
        )
        .getInfo()
        .get("B2")
    )

    if pixel_coverage is None or pixel_coverage < 0.7:
        return None

    year = ee.Date(end_date).get("year")
    median_image = median_image.set(
        {
            "system:time_start": ee.Date.fromYMD(year, 9, 1).millis(),
            "date": ee.Date.fromYMD(year, 9, 1).format("YYYY-MM-DD"),
        }
    )

    return median_image


def get_dem(roi: ee.Geometry) -> ee.Image:
    dem_collection = ee.ImageCollection("COPERNICUS/DEM/GLO30")
    terrain_collection = dem_collection.select("DEM").map(
        lambda img: ee.Terrain.products(img)
    )
    terrain = terrain_collection.mosaic()
    return terrain.clip(roi)
