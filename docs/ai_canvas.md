**DSPRO2 AI Project Canvas**  
Spring 2026

<img src="hslu_logo.png" style="width:5cm" alt="image" />

------------------------------------------------------------------------

# Project Information

|                   |                                 |
|:------------------|:--------------------------------|
| **Project Name:** | Glacier Mass Balance Prediction |
| **Team Members:** | Jay Hawkes, Mara Eckart         |

# Strategic Planning Tool

**Box 1: Problem & Stakeholders**  

- <span style="color: blue">**Stakeholder/User:**</span> Climate
  researcher

- <span style="color: blue">**Decision:**</span> The model informs
  climate impact assessments and water resource planning by estimating
  glacier mass balance in unmonitored regions

- <span style="color: blue">**Main Outcome:**</span> Predicting the
  annual mass balance of unmeasured glaciers using satellite-derived
  indicators, generalizing beyond the manually monitored subset

**Box 2: Context & Project Type**  

- <span style="color: blue">**Project type:**</span> Research, with
  technical prototype component

- <span style="color: blue">**Focus:**</span> Scientific prediction and
  analysis

- <span style="color: blue">**Scope:**</span> Not intended for
  commercial deployment

- <span style="color: blue">**DSPRO2 focus:**</span> Reproducible ML
  pipeline and usable prototype

**Box 3: Describe the Data**  

- <span style="color: blue">**Sources:**</span> GLAMOS, MeteoSwiss,
  Google Earth Engine

- <span style="color: blue">**Formats:**</span> CSV, Parquet, NetCDF,
  Shapefile

- <span style="color: blue">**Time range:**</span> 1984–2025

- <span style="color: blue">**Completeness:**</span> Medium: variable
  coverage due to clouds

- <span style="color: blue">**Accuracy:**</span> Medium to high:
  reliable gold-standard sources, but satellite data needs validation

- <span style="color: blue">**Consistency:**</span> Medium: spatially
  consistent, but lacks temporal consistency between static topography
  and historical data

- <span style="color: blue">**Timeliness:**</span> High, combines
  extensive historical coverage with recent observations

**Box 4: ML Approach**  

- <span style="color: blue">**Method Family:**</span> Supervised
  Regression

- <span style="color: blue">**Justification:**</span> The annual glacier
  mass balance is a continuous target variable and we have access to
  historically labeled datasets to train the model to map
  spatial/meteorological features to these continuous values

**Box 5: ML Pipeline**  

- <span style="color: blue">**Collection:**</span> Automated ingestion
  from GLAMOS, MeteoSwiss API and Google Earth Engine API

- <span style="color: blue">**Preparation:**</span> Cleaning, filtering
  and spatial aggregation of features

- <span style="color: blue">**Train/test split:**</span>
  GroupShuffleSplit by glacier ID

- <span style="color: blue">**Validation:**</span> GroupKFold
  cross-validation by glacier ID

- <span style="color: blue">**Model training:**</span> Random Forest and
  XGBoost

- <span style="color: blue">**Evaluation:**</span> Model evaluation on
  unseen glaciers measured using MAE, RMSE, CVRMSE, *R*<sup>2</sup> and
  a baseline comparison

- <span style="color: blue">**Experiment tracking:**</span> Transparent
  Weights & Biases tracking with feature importance plots and uv.lock to
  ensure reproducibility

**Box 6: Success Definition**  

- <span style="color: blue">**Technical metric:**</span> Root Mean
  Squared Error (RMSE) evaluated on the hold-out test set of unseen
  glaciers and compared to a baseline.

- <span style="color: blue">**Stakeholder KPI:**</span> Time and
  resources saved. Achieving a low enough error rate on unseen glaciers
  proves that the model can reliably supplement manual monitoring of
  annual mass balance

**Box 7: Risks & Guardrails**  

- <span style="color: blue">**EU AI Act risk:**</span> Minimal risk, as
  it is environmental modeling without personal or sensitive human data

- <span style="color: blue">**Risks:**</span>

  - Poor generalization to unseen glaciers

  - Bias toward frequently observed glaciers

  - Cloud-contaminated or unbalanced satellite data observations

- <span style="color: blue">**Guardrails:**</span>

  - Tracking feature importance plots, allowing domain experts to verify
    that the learned relationships make physical sense

  - Human expert review before interpretation or decision-making

**Box 8: Infrastructure**  

- <span style="color: blue">**Strategy:**</span> Hybrid cloud and local
  processing

- <span style="color: blue">**Justification:**</span> Google Cloud and
  the GEE API are necessary for accessing and processing of the large
  satellite images. Local processing is done to build the dataset and
  for the actual model training and hyperparameter tuning.

**Box 9: Costs & Timeline**  

- <span style="color: blue">**Model training costs:**</span> Low
  expected costs

- <span style="color: blue">**Compute:**</span> No GPU required;
  CPU-based training sufficient for dataset

- <span style="color: blue">**Storage:**</span> Processed Parquet data
  and raw source files stored locally

- <span style="color: blue">**Labeling:**</span> No labeling costs;
  labels from GLAMOS

- <span style="color: blue">**Tracking:**</span> Weights & Biases

- <span style="color: blue">**Timeline:**</span> One semester of 14
  weeks

- <span style="color: blue">**Main phases:**</span>

  - Feature engineering: 9 weeks

  - Baseline modeling: 1 week

  - Cross-validation and evaluation: 1–2 weeks

  - Documentation and final presentation: 1–2 weeks

**Box 10: Team & Next Steps**  

- <span style="color: blue">**Project Owner:**</span> Jay Hawkes, Mara
  Eckart

- <span style="color: blue">**Data Engineer:**</span> Jay Hawkes, Mara
  Eckart

- <span style="color: blue">**ML Engineer:**</span> Jay Hawkes, Mara
  Eckart

- <span style="color: blue">**Next step:**</span> Model and feature
  evaluation
