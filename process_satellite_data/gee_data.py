import ee

def initialize_gee(project_id):
    ee.Authenticate()
    ee.Initialize(project=project_id)


def get_satellite_data(roi, year):

    #!!!!can be adapted!!
    # SENTINEL (least cloudy summer image)
    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(roi)
        .filterDate(f"{year}-08-01", f"{year}-09-30") #!!!!can be adapted!!
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
        .sort("CLOUDY_PIXEL_PERCENTAGE")
    )

    image = ee.Image(collection.first()).divide(10000)

    # DEM
    dem = (
        ee.ImageCollection("COPERNICUS/DEM/GLO30")
        .select("DEM")
        .mosaic()
    )

    return image, dem