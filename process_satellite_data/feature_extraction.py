import ee
import numpy as np


def extract_glacier_features(image, dem, roi):
    #!!!!can be adapted!!
    ice_mask = (
        image.normalizedDifference(["B3", "B11"])
        .gt(0.4)
        .And(image.select("B8").gt(0.11))
        .rename("ice_mask")
    )

    area_image = ice_mask.multiply(ee.Image.pixelArea()).rename("ice_mask")
    area_dict = area_image.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=roi,
        scale=10,
        maxPixels=1e9,
    )
    area_m2 = area_dict.get("ice_mask")
    area_km2 = ee.Number(area_m2).divide(1e6).getInfo() if area_m2 else 0.0

    sla_dict = dem.updateMask(ice_mask).reduceRegion(
        #!!!!can be adapted!!
        reducer=ee.Reducer.percentile([5]),
        geometry=roi,
        scale=30,
        maxPixels=1e9,
    )
    sla_m = sla_dict.get("DEM")

    pixel_data = (
        ice_mask
        .unmask(0)
        .sampleRectangle(region=roi)
        .get("ice_mask")
        .getInfo()
    )

    return {
        "ext_area_km2": area_km2,
        "ext_sla_m": ee.Number(sla_m).getInfo() if sla_m else None,
        "pixel_mask": np.array(pixel_data) if pixel_data is not None else None,
    }