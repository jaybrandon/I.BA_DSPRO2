# I.BA_DSPRO2: Glacier Mass Balance Prediction

![Python Version](https://img.shields.io/badge/python-3.13-blue.svg)
![Dependencies](https://img.shields.io/badge/dependencies-uv-green)

A data science project aimed at predicting the annual mass balance of glaciers using satellite, meteorological, and topographical data.

## Table of Contents
- [Project Overview](#project-overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Dataset Generation](#1-dataset-generation)
  - [Model Training](#2-model-training)
- [Project Structure](#project-structure)

## Project Overview
This project uses Machine Learning (Random Forest and XGBoost) to estimate the annual mass balance of glaciers. It aggregates data from various sources:
- **GLAMOS:** Glacier mass balance observational data.
- **MeteoSwiss:** Meteorological and climate data.
- **Satellite Data (Google Earth Engine):** Imagery features and indices.

## Requirements
- **Python:** >= 3.13
- **Package Manager:** [uv](https://github.com/astral-sh/uv)
- **Google Cloud Project:** With access to Google Earth Engine API.

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/jaybrandon/I.BA_DSPRO2.git
   cd I.BA_DSPRO2
   ```

2. **Install dependencies using `uv`:**
   ```bash
   uv sync
   ```
   *This will create a virtual environment (`.venv`) and install all required dependencies listed in `pyproject.toml` and `uv.lock`.*

## Configuration

The dataset generation relies on Google Earth Engine which requires a Google Cloud Project ID. 

1. Create a `.env` file in the root directory:
   ```bash
   touch .env
   ```
2. Add your Google Cloud Project ID to the `.env` file:
   ```env
   GC_PROJECT_ID=your-google-cloud-project-id
   ```

You also need to authenticate with Google Earth Engine:
```bash
uv run earthengine authenticate
```

For model tracking and logging, this project uses [Weights & Biases (WandB)](https://wandb.ai). Make sure you are logged in to WandB:
```bash
uv run wandb login
```

## Usage

### 1. Dataset Generation

The dataset compilation is handled by `src/dataset/dataset.py`. It fetches, cleans, and merges GLAMOS, MeteoSwiss, and Satellite data, saving the processed dataset as a Parquet file in `data/processed/`. Please refer to our [Datasheet](docs/datasheet.md) for detailed information about the dataset.

**Command:**
```bash
uv run src/dataset/dataset.py [OPTIONS]
```

**Options:**
- `--start-year INTEGER`: The start year for data processing.
- `--end-year INTEGER`: The end year for data processing.
- `--skip-dl / --no-skip-dl`: Skip downloading meteorological data if it's already cached.

**Example:**
```bash
uv run src/dataset/dataset.py --start-year 1984 --end-year 2025
```
*Note: Ensure your `.env` file has `GC_PROJECT_ID` configured before running.*

### 2. Model Training

The training pipeline is located at `src/train/train.py` and uses [Hydra](https://hydra.cc/) for configuration management. Model and training configurations can be found in `src/train/conf/`.

**Command:**
```bash
uv run -m src.train.train +model=MODEL
```
It is required that you override the model using `+model=MODEL` with
- `xgb` for XGBoost
- `rfr` for Random Forest

**Overriding Configurations:**
Because the project uses Hydra, you can easily override configuration parameters directly from the command line without modifying the YAML files:

```bash
# Change the random seed and override wandb project name using XGBoost
uv run -m src.train.train +model=xgb seed=42 wandb.project="glacier-mass-balance"

# Use a different dataset version using Random Forest
uv run -m src.train.train +model=rfr dataset="v3.3"
```

## Project Structure

```text
├── data/
│   ├── processed/          # Processed parquet datasets
│   └── raw/                # Raw downloaded data
├── docs/                   # Documentation and AI Canvas
├── models/                 # Saved model weights and predictions
├── notebooks/              # Jupyter notebooks for data exploration and QA
├── src/                    # Source code
│   ├── dataset/            # Data collection and feature extraction scripts
│   ├── train/              # Model training, evaluation, and Hydra configs
│   └── util.py             # Utility functions
├── pyproject.toml          # Project metadata and dependencies
├── uv.lock                 # Lockfile for reproducible builds
└── README.md               # Project documentation
```