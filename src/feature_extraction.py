import ee

def add_glacier_masks(image):
    ndsi = image.select("NDSI")
    snow_ice_mask = (
        ndsi.gt(0.4)
        .And(image.select("B8").gt(0.11))
        .rename("snow_ice_mask")
    )
    return image.addBands(snow_ice_mask)

def extract_per_image_features(image, dem, polygon, obs_id):
    
    image = add_glacier_masks(image)
    mask = image.select("snow_ice_mask")
    

    stats = image.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=polygon,
        scale=30,
        maxPixels=1e9
    )
    
    dem_stats = dem.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=polygon,
        scale=30,
        maxPixels=1e9
    )

    area = mask.multiply(ee.Image.pixelArea()).reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=polygon,
        scale=30,
        maxPixels=1e9
    ).get("snow_ice_mask")

    sla = dem.select("DEM").updateMask(mask).reduceRegion(
        reducer=ee.Reducer.percentile([5]),
        geometry=polygon,
        scale=30,
        maxPixels=1e9
    ).get("DEM")

    
    return ee.Feature(None, {
        "obs_id": obs_id,
        "date": image.date().format("YYYY-MM-DD"),
        "B2": stats.get("B2"),
        "B3": stats.get("B3"),
        "B4": stats.get("B4"),
        "B8": stats.get("B8"),
        "B11": stats.get("B11"),
        "B12": stats.get("B12"),
        "NDSI": stats.get("NDSI"),
        "snow_fraction": stats.get("snow_ice_mask"),
        "area_m2": area,
        "sla": sla,
        "elev_mean": dem_stats.get("DEM"),
        "slope_mean": dem_stats.get("slope"),
        "aspect_mean": dem_stats.get("aspect")
    })

def extract_glacier_period_features(collection, dem, polygon, obs_id):
    feature_col = collection.map(lambda img: extract_per_image_features(img, dem, polygon, obs_id))
    return feature_col.getInfo()['features']