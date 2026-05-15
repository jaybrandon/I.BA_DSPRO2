import math

import ee
from retry import retry

SCR_SLA_THRESHOLD = 0.4


def extract_sla(
    snow_mask: ee.Image, ice_mask: ee.Image, dem: ee.Image, roi: ee.Geometry
) -> ee.Number:
    dem_bins = dem.select("DEM").divide(10).floor().multiply(10).rename("elevation")
    combined = ee.Image.cat([snow_mask, ice_mask, dem_bins])

    stats = combined.reduceRegion(
        reducer=ee.Reducer.sum().repeat(2).group(groupField=2, groupName="elevation"),
        geometry=roi,
        scale=30,
        maxPixels=1e9,
    )

    groups = ee.List(stats.get("groups", ee.List([])))

    def process_bin(b):
        b_dict = ee.Dictionary(b)
        elev = ee.Number(b_dict.get("elevation"))
        counts = ee.List(b_dict.get("sum"))
        snow = ee.Number(counts.get(0))
        ice = ee.Number(counts.get(1))

        total = snow.add(ice)

        scr = ee.Algorithms.If(total.gt(0), snow.divide(total), -1)

        return ee.Feature(None, {"elevation": elev, "scr": scr})

    valid_bins = (
        ee.FeatureCollection(groups.map(process_bin))
        .filter(ee.Filter.gte("scr", 0))
        .sort("elevation")
    )

    elevs = valid_bins.aggregate_array("elevation")
    scrs = valid_bins.aggregate_array("scr")

    mask_list = scrs.map(lambda val: ee.Number(val).gte(SCR_SLA_THRESHOLD))

    fallback_idx = mask_list.indexOf(1)
    fallback = ee.Algorithms.If(fallback_idx.neq(-1), elevs.get(fallback_idx), -9999)

    def find_streak():
        mask_arr = ee.Array(mask_list)
        length = mask_arr.length().get([0])

        a0 = mask_arr.slice(0, 0, length.subtract(2))
        a1 = mask_arr.slice(0, 1, length.subtract(1))
        a2 = mask_arr.slice(0, 2, length)

        streak_sums = a0.add(a1).add(a2).toList()

        sla_idx = streak_sums.indexOf(3)

        return ee.Algorithms.If(
            sla_idx.neq(-1),
            elevs.get(sla_idx),
            fallback,
        )

    sla = ee.Algorithms.If(mask_list.size().gte(3), find_streak(), fallback)

    return ee.Number(sla)


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


def add_glacier_masks(image: ee.Image) -> ee.Image:
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

    dem_minmax = dem.reduceRegion(
        reducer=ee.Reducer.minMax(), geometry=polygon, scale=30, maxPixels=1e9
    )

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
            "elev_min": dem_minmax.get("DEM_min"),
            "elev_max": dem_minmax.get("DEM_max"),
        },
    )


@retry(tries=10, delay=1, backoff=2)
def extract_glacier_period_features(
    composite: ee.Image, dem: ee.Image, polygon: ee.Geometry, obs_id: str
):
    feature = extract_per_image_features(composite, dem, polygon, obs_id).getInfo()

    props = feature["properties"]
    if props.get("SLA") == -9999:
        props["SLA"] = None

    masks = (
        add_glacier_masks(composite)
        .select(["ndsi_mask", "snow_mask", "ice_mask"])
        .set("system:index", ee.String(str(obs_id)))
    )

    return {
        "features": [props],
        "masks": masks,
    }
