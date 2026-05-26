**Dataset Datasheet**  
**Course:** DSPRO2 FS26  

|                        |                                             |
|:-----------------------|:--------------------------------------------|
| **Project Name:**      | Glacier Mass Balance Prediction             |
| **Student Names:**     | Jay Hawkes, Mara Eckart                     |
| **Date:**              | 2026-05-26                                  |
| **GitHub Repository:** | <https://github.com/jaybrandon/I.BA_DSPRO2> |

# Dataset Overview

- **Official Name:** Glacier Mass Balance Prediction Dataset

- **Owner / Contact:** DSPRO2 Project Team (HSLU)

- **Short Description:** The dataset contains glacier mass balance
  observations combined with glacier geometry, meteorological data,
  satellite-derived features, and elevation information. It is used to
  build a machine learning model for predicting annual glacier mass
  balance on unseen glaciers.

- **Version & Date:** v3.3, Project integration state: May 2026

- **Reference:** Internal project documentation

# Motivation and Intended Use

- **Primary Purpose:** The dataset was created to combine fragmented
  glacier-related data sources into a unified analytical dataset to
  predict annual glacier mass balance for research purposes.

- **Intended Use:**

  - Annual glacier mass balance prediction

  - Feature-based regression using satellite and meteorological data

  - Analysis of glacier–climate relationships

  - Generalization testing on unseen glaciers

- **Out-of-Scope Use:** This dataset is not intended for commercial
  deployment, or direct environmental decision-making without expert
  validation.

# Composition and Coverage

- **Data Types:** Structured (tabular Parquet files) and
  Multi-dimensional (NetCDF only for meteorological data).

- **Population:** Swiss glaciers listed in the GLAMOS inventory with
  recorded mass balance observations between 1961 and 2025.

- **Dataset Size:** Final Sample Size: 464 observations across
  approximately 30 features. The time span covers 1984–2025, due to
  satellite coverage (Landsat/Sentinel)

- **Known Gaps / Imbalances:** Data is limited by cloud cover and
  inconsistant quality in satellite imagery. Strong imbalance of number
  of observations per glacier. Some are less frequently observed than
  others with a maximum difference of 511 observations.

# Sources and Collection

- **Provenance:** Public open data from GLAMOS (Glacier Monitoring
  Switzerland), MeteoSwiss (RhiresM/TabsM datasets), and satellite
  imagery via Google Earth Engine (Landsat 5/8/9, Sentinel-2).

- **Collection Method:** Automated API ingestion (Google Earth Engine),
  automated web scraping/downloads from the Swiss Federal Geoportal
  (data.geo.admin.ch), and CSV/Parquet processing.

- **Jurisdictions:** Switzerland (specifically the Swiss Alps).

# Labels and Targets

- **Target Definition:** Annual Glacier Mass Balance (measured in mm
  w.e. – millimeters of water equivalent).

- **Labeling Process:** Labels are assigned based on official field
  measurements and glaciological modeling provided by GLAMOS.

- **Quality Control:**

  - Consistency checks between glacier ID, year, and source joins

  - Manual validation of preprocessing outputs such as masks and
    extracted satellite features

# Quality Assessment (Data Audit)

- **Preprocessing Steps:**

  - Filter and clean raw data by removing incomplete or invalid
    observations

  - Clip data to relevant regions of interest and suitable temporal
    ranges

  - Derive features from available data sources

  - Integrate multiple data sources into a single dataset

  - Handling missing values

- **Initial Audit Findings:** Removal of outliers where
  satellite-derived area measurements were zero, most likely due to
  cloud-masking errors.

- **Quality Dimensions:**

  - **Completeness:** Low to Medium, as coverage varies across glaciers
    and time periods. In particular, satellite-derived features are
    unavailable before 1984, and revisit intervals as well as image
    quality lead to an uneven number of valid observations.

  - **Accuracy:** Medium. The underlying data sources are highly
    reliable, with GLAMOS and Meteoswiss serving as a gold-standard
    reference, but some derived satellite-based features still require
    careful validation as they are prone to be noisy due to unclear and
    cloud covered images.

  - **Consistency:** Medium. While spatial consistency is maintained via
    the EPSG:2056 coordinate system, there is limited temporal
    consistency between the static DEM topography and the historical
    satellite observations.

  - **Timeliness:** High, as the dataset combines long historical
    coverage with recent observations and is well suited to the project
    context.

# Legal, Ethical, and Governance

- **Sensitive Data:**

  - The dataset does not contain direct personal data

  - It contains glacier, terrain, weather, and remote sensing
    information only

- **Compliance:**

  - The project is for academic purposes within the DSPRO2 course

  - Swiss and European data protection laws are not expected to be
    considered because no personal data is intentionally processed and
    the data is publicly accessible.

- **Consent & Licensing:**

  - Access to satellite imagery via Google Earth Engine (GEE) was
    granted under a specific non-commercial, academic use agreement.

  - Data from GLAMOS and MeteoSwiss are utilized under their respective
    institutional open data terms, intended for scientific and
    educational use.

  - Due to the GEE access agreement and source data terms, this
    integrated dataset is strictly restricted to non-commercial,
    academic research.

  - If published, the dataset must carry a restrictive license and
    cannot be distributed under permissive commercial licenses.

# Dataset Structure and Schema

- **File Formats:**

  - Raster formats for DEM and satellite imagery

  - NetCDF for MeteoSwiss meteorological data

  - CSV / Shapefile for Glamos data

  - Parquet for the final dataset

- **Directory Layout:**

  - `data/raw/` (Immutable raw data files)

  - `data/processed/` (Final serialized Parquet files ready for model
    training)

  - `notebooks/` (Jupyter notebooks for exploratory data analysis and
    baseline prototyping)

  - `src/` (Isolated Python scripts for the data processing pipeline,
    model training, and validation)

  - `models/` (Serialized model artifacts)

  - `docs/` (Project documentation, including the Markdown versions of
    the AI Canvas and this Data Sheet)

- **Schema Summary:**

  - GLAMOS observations as the core label table

  - MeteoSwiss data linked by glacier location and hydrological period

  - Satellite-derived features linked by glacier and observation window
    (July,Aug,Sep)

  - DEM-derived features linked to glacier geometry and masked regions

- **Units & Encodings:**

  - Hydrological year defined from October to September

  - Meteorological data aggregated by hydrological quarters

  - Satellite features based on bands B11, B12, B2, B3, B4, and B8
