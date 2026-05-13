import math

import ee

SCR_SLA_THRESHOLD = 0.6
MIN_CONSECUTIVE_SLA = 3


def extract_sla(
    snow_mask: ee.Image, ice_mask: ee.Image, dem: ee.Image, roi: ee.Geometry
):
    dem_bins = dem.select("DEM").divide(10).floor().multiply(10).rename("elevation")

    combined = ee.Image.cat([snow_mask, ice_mask, dem_bins])

    bin_stats = (
        combined.reduceRegion(
            reducer=ee.Reducer.sum()
            .repeat(2)
            .group(groupField=2, groupName="elevation"),
            geometry=roi,
            scale=30,
            maxPixels=1e9,
        )
        .get("groups")
        .getInfo()
    )

    if bin_stats is None:
        return None

    bin_stats = sorted(bin_stats, key=lambda x: x["elevation"])

    consecutive_sla_count = 0
    fallback_sla = None
    run_elev = None
    sla = None

    for bin in bin_stats:
        elev = bin["elevation"]
        n_snow = bin["sum"][0]
        n_ice = bin["sum"][1]
        total = n_snow + n_ice

        if total == 0:
            continue

        scr = n_snow / total

        if scr >= SCR_SLA_THRESHOLD:
            if fallback_sla is None:
                fallback_sla = elev

            if consecutive_sla_count == 0:
                run_elev = elev

            consecutive_sla_count += 1

            if consecutive_sla_count >= MIN_CONSECUTIVE_SLA:
                sla = run_elev
                break
        else:
            consecutive_sla_count = 0

    return sla if sla is not None else fallback_sla


def get_otsu_threshold(hist):
    counts = ee.Array(ee.Dictionary(hist).get("histogram"))
    means = ee.Array(ee.Dictionary(hist).get("bucketMeans"))
    size = means.length().get([0])
    total = counts.reduce(ee.Reducer.sum(), [0]).get([0])
    sum = means.multiply(counts).reduce(ee.Reducer.sum(), [0]).get([0])
    mean = sum.divide(total)

    indices = ee.List.sequence(1, size)

    def calc_bss(i):
        aCounts = counts.slice(0, 0, i)
        aCount = aCounts.reduce(ee.Reducer.sum(), [0]).get([0])
        aMeans = means.slice(0, 0, i)
        aMean = (
            aMeans.multiply(aCounts)
            .reduce(ee.Reducer.sum(), [0])
            .get([0])
            .divide(aCount)
        )

        bCount = total.subtract(aCount)
        bMean = sum.subtract(aCount.multiply(aMean)).divide(bCount)

        return aCount.multiply(aMean.subtract(mean).pow(2)).add(
            bCount.multiply(bMean.subtract(mean).pow(2))
        )

    bss = indices.map(calc_bss)

    return means.sort(bss).get([-1])


def calc_snow_ice_masks(image: ee.Image):
    swir = image.select("B11")
    nir = image.select("B8")
    nir_new = nir.multiply(nir.divide(swir)).rename("NIRNEW")

    hist = nir_new.reduceRegion(ee.Reducer.histogram(), scale=30, maxPixels=1e9).get(
        "NIRNEW"
    )

    otsu_threshold = get_otsu_threshold(hist)
    snow_mask = nir_new.gte(otsu_threshold).rename("snow_mask")
    ice_mask = nir_new.lt(otsu_threshold).rename("ice_mask")
    return snow_mask, ice_mask


def add_glacier_masks(image: ee.Image):
    ndsi = image.normalizedDifference(["B3", "B11"]).rename("NDSI")
    nir = image.select("B8")
    ndsi_mask = ndsi.gt(0.4).And(nir.gt(0.11)).rename("ndsi_mask")

    snow_mask, ice_mask = calc_snow_ice_masks(image)

    return image.addBands([ndsi, ndsi_mask, snow_mask, ice_mask], overwrite=True)


def extract_per_image_features(
    image: ee.Image, dem: ee.Image, polygon: ee.Geometry, obs_id: str
):

    image = add_glacier_masks(image)
    snow_mask = image.select("snow_mask")
    ice_mask = image.select("ice_mask")

    mean_stats = image.reduceRegion(
        reducer=ee.Reducer.mean(), geometry=polygon, scale=30, maxPixels=1e9
    )

    std_stats = image.reduceRegion(
        reducer=ee.Reducer.stdDev(), geometry=polygon, scale=30, maxPixels=1e9
    )

    snow_area_img = snow_mask.selfMask().multiply(ee.Image.pixelArea())
    ice_area_img = ice_mask.selfMask().multiply(ee.Image.pixelArea())

    snow_stats = snow_area_img.reduceRegion(
        ee.Reducer.sum(), geometry=polygon, scale=30, maxPixels=1e9
    )
    ice_stats = ice_area_img.reduceRegion(
        ee.Reducer.sum(), geometry=polygon, scale=30, maxPixels=1e9
    )

    snow_area = ee.Number(snow_stats.get("snow_mask"))
    ice_area = ee.Number(ice_stats.get("ice_mask"))

    total_area = snow_area.add(ice_area)
    scr = snow_area.divide(total_area)

    sla = extract_sla(snow_mask, ice_mask, dem, polygon)

    aspect_rad = dem.select("aspect").multiply(math.pi / 180.0)
    eastness = aspect_rad.sin().rename("eastness")
    northness = aspect_rad.cos().rename("northness")

    combined_dem = dem.select(["DEM", "slope"]).addBands([eastness, northness])

    dem_stats = combined_dem.reduceRegion(
        reducer=ee.Reducer.mean(), geometry=polygon, scale=30, maxPixels=1e9
    )

    mean_east = ee.Number(dem_stats.get("eastness"))
    mean_north = ee.Number(dem_stats.get("northness"))

    aspect_rad = mean_east.atan2(mean_north)
    aspect_deg = aspect_rad.multiply(180.0 / math.pi)
    aspect = aspect_deg.mod(360)

    return ee.Feature(
        None,
        {
            "obs_id": obs_id,
            "date": image.date().format("YYYY-MM-DD"),
            "B2_mean": mean_stats.get("B2"),
            "B3_mean": mean_stats.get("B3"),
            "B4_mean": mean_stats.get("B4"),
            "B8_mean": mean_stats.get("B8"),
            "B11_mean": mean_stats.get("B11"),
            "B12_mean": mean_stats.get("B12"),
            "B2_std": std_stats.get("B2"),
            "B3_std": std_stats.get("B3"),
            "B4_std": std_stats.get("B4"),
            "B8_std": std_stats.get("B8"),
            "B11_std": std_stats.get("B11"),
            "B12_std": std_stats.get("B12"),
            "SCR": scr,
            "SCA": snow_area,
            "SLA": sla,
            "elev_mean": dem_stats.get("DEM"),
            "slope_mean": dem_stats.get("slope"),
            "aspect_mean": aspect,
        },
    )


def extract_glacier_period_features(
    collection: ee.ImageCollection, dem: ee.Image, polygon: ee.Geometry, obs_id: str
):
    count = collection.size()

    img_list = collection.toList(count)
    features = []

    for i in range(count.getInfo()):  # ty:ignore[invalid-argument-type]
        img = ee.Image(img_list.get(i))

        feature = extract_per_image_features(img, dem, polygon, obs_id)
        features.append(feature)

    masks = (
        collection.map(add_glacier_masks)
        .select(["ndsi_mask", "snow_mask", "ice_mask"])
        .median()
        .gt(0.5)
        .clip(polygon)
    ).toByte()

    return {
        "features": ee.FeatureCollection(features).getInfo()["features"],
        "masks": masks,
    }
