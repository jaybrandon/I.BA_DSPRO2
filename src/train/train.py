import os
from pathlib import Path

import hydra
import numpy as np
import pandas as pd
from omegaconf import DictConfig, OmegaConf
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error
from sklearn.model_selection import GroupKFold, GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from wandb.sdk import Config

import wandb
from src.util import set_seed

DATA_DIR = Path(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    + "/data/"
)

TARGET = "mass_balance_annual"
CATEGORICAL_FEATURES = ["satellite"]
NUMERICAL_FEATURES = [
    "sla_norm",
    "elev_mean",
    "slope_mean",
    "aspect_mean",
    "snow_fraction",
    "B2",
    "B3",
    "B4",
    "B8",
    "B11",
    "B12",
    "q1h_temp",
    "q2h_temp",
    "q3h_temp",
    "q4h_temp",
    "q1h_prec",
    "q2h_prec",
    "q3h_prec",
    "q4h_prec",
]


def load_dataset(version: str) -> pd.DataFrame:
    path = DATA_DIR / "processed" / f"glacier_ml_dataset_{version}.parquet"
    df = pd.read_parquet(path)
    return df


def train_rfr(
    conf: Config, X_train: pd.DataFrame, y_train: pd.Series, groups_train: pd.Series
):
    model_conf = conf["model"].copy()
    del model_conf["name"]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", "passthrough", NUMERICAL_FEATURES),
            (
                "cat",
                OneHotEncoder(
                    drop="first", sparse_output=False, handle_unknown="ignore"
                ),
                CATEGORICAL_FEATURES,
            ),
        ]
    )

    rfr = RandomForestRegressor(**model_conf, random_state=conf["seed"], n_jobs=-1)

    pipeline = Pipeline(steps=[("preprocessor", preprocessor), ("regressor", rfr)])

    X_train_reset = X_train.reset_index(drop=True)
    y_train_reset = y_train.reset_index(drop=True)
    groups_train_reset = groups_train.reset_index(drop=True)

    gkf = GroupKFold(n_splits=5)
    fold_rmse = []
    fold_mae = []
    fold_r2 = []

    for fold, (train_idx, val_idx) in enumerate(
        gkf.split(X_train_reset, y_train_reset, groups_train_reset)
    ):
        X_tr, X_val = X_train_reset.iloc[train_idx], X_train_reset.iloc[val_idx]
        y_tr, y_val = y_train_reset.iloc[train_idx], y_train_reset.iloc[val_idx]

        pipeline.fit(X_tr, y_tr)
        preds = pipeline.predict(X_val)

        rmse = root_mean_squared_error(y_val, preds)
        mae = mean_absolute_error(y_val, preds)
        r2 = r2_score(y_val, preds)

        fold_rmse.append(rmse)
        fold_mae.append(mae)
        fold_r2.append(r2)

    avg_val_rmse = np.mean(fold_rmse)
    avg_val_mae = np.mean(fold_mae)
    avg_val_r2 = np.mean(fold_r2)

    print(f"Average Validation RMSE: {avg_val_rmse:.4f}")
    print(f"Average Validation MAE: {avg_val_mae:.4f}")
    print(f"Average Validation R2: {avg_val_r2:.4f}")

    wandb.log({"val_rmse": avg_val_rmse, "val_mae": avg_val_mae, "val_r2": avg_val_r2})


def train_xgb(conf):
    pass


@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    set_seed(cfg.seed)

    df = load_dataset(version=cfg.dataset)

    X = df[CATEGORICAL_FEATURES + NUMERICAL_FEATURES]
    y = df[TARGET]
    groups = df["id"]

    gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=cfg.seed)
    train_idx, test_idx = next(gss.split(X, y, groups))

    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
    groups_train = groups.iloc[train_idx]
    groups_test = groups.iloc[test_idx]

    with wandb.init(
        entity=cfg.wandb.entity,
        project=cfg.wandb.project,
        config=OmegaConf.to_container(cfg, resolve=True, throw_on_missing=True),  # ty:ignore[invalid-argument-type]
    ) as run:
        conf = run.config

        if conf["model"]["name"] == "rfr":
            train_rfr(conf, X_train, y_train, groups_train)
        else:
            train_xgb(conf)


if __name__ == "__main__":
    main()
