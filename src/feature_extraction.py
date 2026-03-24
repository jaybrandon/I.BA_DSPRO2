import ee
import numpy as np


def add_glacier_masks(image):
    ndsi = image.select("NDSI")
    snow_ice_mask = (
        ndsi.gt(0.4)
        .And(image.select("B8").gt(0.11))
        .rename("snow_ice_mask")
    )
    return image.addBands(snow_ice_mask)


def add_image_properties(image, dem, polygon):
    mask = image.select("snow_ice_mask")

    area_m2 = (
        mask.multiply(ee.Image.pixelArea())
        .reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=polygon,
            scale=10,
            maxPixels=1e9
        )
        .get("snow_ice_mask")
    )

    sla_p05 = (
        dem.select("DEM")
        .updateMask(mask)
        .reduceRegion(
            reducer=ee.Reducer.percentile([5]),
            geometry=polygon,
            scale=30,
            maxPixels=1e9
        )
        .get("DEM")
    )

    fraction = mask.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=polygon,
        scale=10,
        maxPixels=1e9
    ).get("snow_ice_mask")

    return image.set({
        "snow_ice_area_m2": area_m2,
        "snow_ice_fraction": fraction,
        "sla": sla_p05
    })


def extract_glacier_period_features(collection, dem, polygon, glacier_id, observation_start, observation_end):
    collection = collection.map(add_glacier_masks)
    collection = collection.map(lambda img: add_image_properties(img, dem, polygon))

    image_count = collection.size()

    mean_image = collection.mean()
    median_image = collection.median()
    std_image = collection.reduce(ee.Reducer.stdDev())

    scale_10 = 10
    scale_30 = 30

    ndsi_stats = mean_image.select("NDSI").reduceRegion(
        reducer=ee.Reducer.mean().combine(
            reducer2=ee.Reducer.minMax(),
            sharedInputs=True
        ),
        geometry=polygon,
        scale=scale_10,
        maxPixels=1e9
    )

    reflectance_stats = median_image.select(["B2", "B3", "B4", "B8", "B11", "B12"]).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=polygon,
        scale=scale_10,
        maxPixels=1e9
    )

    ndsi_std_stats = std_image.select("NDSI_stdDev").reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=polygon,
        scale=scale_10,
        maxPixels=1e9
    )

    mean_mask_fraction = mean_image.select("snow_ice_mask").reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=polygon,
        scale=scale_10,
        maxPixels=1e9
    )

    dem_stats = dem.reduceRegion(
        reducer=ee.Reducer.mean().combine(
            reducer2=ee.Reducer.minMax(),
            sharedInputs=True
        ),
        geometry=polygon,
        scale=scale_30,
        maxPixels=1e9
    )

    area_list = ee.List(collection.aggregate_array("snow_ice_area_m2"))
    fraction_list = ee.List(collection.aggregate_array("snow_ice_fraction"))
    sla_list = ee.List(collection.aggregate_array("sla"))

    mean_snow_ice_area_m2 = ee.Number(area_list.reduce(ee.Reducer.mean()))
    max_snow_ice_area_m2 = ee.Number(area_list.reduce(ee.Reducer.max()))
    min_snow_ice_area_m2 = ee.Number(area_list.reduce(ee.Reducer.min()))

    mean_snow_ice_fraction = ee.Number(fraction_list.reduce(ee.Reducer.mean()))
    min_snow_ice_fraction = ee.Number(fraction_list.reduce(ee.Reducer.min()))
    max_snow_ice_fraction = ee.Number(fraction_list.reduce(ee.Reducer.max()))

    mean_sla = ee.Number(sla_list.reduce(ee.Reducer.mean()))
    min_sla = ee.Number(sla_list.reduce(ee.Reducer.min()))
    max_sla = ee.Number(sla_list.reduce(ee.Reducer.max()))
    std_sla = ee.Number(sla_list.reduce(ee.Reducer.stdDev()))

    final_mask = median_image.select("snow_ice_mask")
    mean_ndsi_image = mean_image.select("NDSI")

    feature_dict = ee.Dictionary({
        "glacier_id": glacier_id,
        "observation_start": observation_start,
        "observation_end": observation_end,
        "image_count": image_count,
        "mean_snow_ice_area_km2": mean_snow_ice_area_m2.divide(1e6),
        "max_snow_ice_area_km2": max_snow_ice_area_m2.divide(1e6),
        "min_snow_ice_area_km2": min_snow_ice_area_m2.divide(1e6),
        "mean_snow_ice_fraction": mean_snow_ice_fraction,
        "min_snow_ice_fraction": min_snow_ice_fraction,
        "max_snow_ice_fraction": max_snow_ice_fraction,
        "mean_sla": mean_sla,
        "min_sla": min_sla,
        "max_sla": max_sla,
        "std_sla": std_sla,
    }).combine(ndsi_stats).combine(reflectance_stats).combine(ndsi_std_stats).combine(mean_mask_fraction).combine(dem_stats)

    return {
        "features": feature_dict.getInfo(),
        "final_mask_image": final_mask,
        "mean_ndsi_image": mean_ndsi_image,
    }