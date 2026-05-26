import ee

def add_glacier_masks(image):
    ndsi = image.normalizedDifference(["B3", "B11"]).rename("NDSI")
    nir = image.select("B8")
    mask = ndsi.gt(0.4).And(nir.gt(0.11)).rename("mask")
    return image.addBands([ndsi, mask], overwrite=True)

def extract_per_image_features(image, dem, polygon, obs_id):
    
    image = add_glacier_masks(image)
    mask = image.select("mask")
    

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

    # area = mask.multiply(ee.Image.pixelArea()).reduceRegion(
    #     reducer=ee.Reducer.sum(),
    #     geometry=polygon,
    #     scale=30,
    #     maxPixels=1e9
    # ).get("mask")
  
    area = (
        mask.rename("mask")
        .selfMask()
        .multiply(ee.Image.pixelArea())
        .reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=polygon,
            scale=30,
            maxPixels=1e9
        )
        .get("mask")
    )

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
        "snow_fraction": stats.get("mask"),
        "area_m2": area,
        "sla": sla,
        "elev_mean": dem_stats.get("DEM"),
        "slope_mean": dem_stats.get("slope"),
        "aspect_mean": dem_stats.get("aspect")
    })

def extract_glacier_period_features(collection, dem, polygon, obs_id):
    feature_col = collection.map(lambda img: extract_per_image_features(img, dem, polygon, obs_id))
    masked_collection = collection.map(add_glacier_masks)
    #final_mask = masked_collection.median().select("mask")
    final_mask = (
        collection
        .map(add_glacier_masks)
        .select("snow_ice_mask")
        .median()
        .gt(0.5)
        .selfMask()
        .rename("snow_ice_mask")
        .clip(polygon)
    )
    
    return {
        "features": feature_col.getInfo()['features'], 
        "final_mask_image": final_mask }