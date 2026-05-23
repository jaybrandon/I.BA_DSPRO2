import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import hydra
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import xgboost as xgb
from omegaconf import DictConfig, OmegaConf, open_dict
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error
from sklearn.model_selection import GroupKFold, GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from wandb.sdk import Config
from xgboost import XGBRegressor

import wandb
from src.util import set_seed

BASE_DIR = Path(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
DATA_DIR = BASE_DIR / "data"

TARGET = "mass_balance_annual"
CATEGORICAL_FEATURES = ["satellite"]
NUMERICAL_FEATURES = [
    "sla_norm",
    "B2_mean",
    "B3_mean",
    "B4_mean",
    "B8_mean",
    "B11_mean",
    "B12_mean",
    "B11_std",
    "B12_std",
    "SCR",
    "SCA",
    "slope_mean",
    "q1h_temp",
    "q2h_temp",
    "q3h_temp",
    "q4h_temp",
    "q1h_prec",
    "q3h_prec",
]


def load_dataset(version: str) -> pd.DataFrame:
    path = DATA_DIR / "processed" / f"glacier_ml_dataset_{version}.parquet"
    df = pd.read_parquet(path)
    df[CATEGORICAL_FEATURES] = df[CATEGORICAL_FEATURES].astype("category")
    return df


def calc_metrics(target, preds, baseline_mean, baseline_median, prefix):
    mean = target.mean()
    std = target.std()

    rmse_baseline = root_mean_squared_error(target, baseline_mean)
    rmse = root_mean_squared_error(target, preds)
    cvrmse = rmse / mean if mean != 0 else 0

    mae_baseline = mean_absolute_error(target, baseline_median)
    mae = mean_absolute_error(target, preds)

    r2 = r2_score(target, preds)

    return {
        f"{prefix}_mean": mean,
        f"{prefix}_std": std,
        f"{prefix}_rmse_baseline": rmse_baseline,
        f"{prefix}_rmse": rmse,
        f"{prefix}_cvrmse": cvrmse,
        f"{prefix}_mae_baseline": mae_baseline,
        f"{prefix}_mae": mae,
        f"{prefix}_r2": r2,
    }


def log_perm_feature_importance(
    run: wandb.Run, model: RandomForestRegressor | Pipeline | XGBRegressor, X, y
):
    f, ax = plt.subplots(figsize=(10, 8))

    result = permutation_importance(
        model, X, y, n_repeats=10, random_state=run.config["seed"], n_jobs=-1
    )
    forest_importances = pd.DataFrame(
        [model.feature_names_in_, result.importances_mean]
    ).T
    forest_importances.columns = ["feature", "importance"]

    sns.barplot(
        forest_importances.sort_values("importance", ascending=False),
        x="importance",
        y="feature",
        orient="h",
        ax=ax,
    )
    ax.set_title("Permutation Importance")
    plt.tight_layout()

    run.log({"permutation_importance": wandb.Image(f)})
    plt.close(f)


def log_xgb_feature_importance(run: wandb.Run, bst: xgb.Booster, type: str):
    f, ax = plt.subplots(figsize=(10, 8))

    xgb.plot_importance(
        booster=bst,
        ax=ax,
        importance_type=type,
        title=f"XGBoost Feature Importance ({type})",
    )
    plt.tight_layout()

    run.log({f"feature_importance_{type}": wandb.Image(f)})
    plt.close(f)


def train_rfr(
    conf: Config, X_train: pd.DataFrame, y_train: pd.Series, seed: int
) -> Pipeline:
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

    rfr = RandomForestRegressor(**conf, random_state=seed, n_jobs=-1)

    pipeline = Pipeline(steps=[("preprocessor", preprocessor), ("regressor", rfr)])

    pipeline.fit(X_train, y_train)

    return pipeline


def train_xgb(
    conf: Config,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    seed: int,
) -> XGBRegressor:
    conf = conf.copy()
    early_stopping_rounds = conf.pop("early_stopping_rounds", None)
    n_estimators = conf.pop("num_boost_round", 100)

    model = XGBRegressor(
        n_estimators=n_estimators,
        early_stopping_rounds=early_stopping_rounds,
        enable_categorical=True,
        random_state=seed,
        **conf,
    )

    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=True)

    return model


def cross_validate(
    run: wandb.Run, X_train: pd.DataFrame, y_train: pd.Series, groups_train: pd.Series
):
    conf = run.config
    model_conf = conf["model"].copy()
    del model_conf["name"]

    X_train_reset = X_train.reset_index(drop=True)
    y_train_reset = y_train.reset_index(drop=True)
    groups_train_reset = groups_train.reset_index(drop=True)

    gkf = GroupKFold(n_splits=conf["k_folds"])
    results = []

    for fold, (train_idx, val_idx) in enumerate(
        gkf.split(X_train_reset, y_train_reset, groups_train_reset)
    ):
        X_tr, X_val = X_train_reset.iloc[train_idx], X_train_reset.iloc[val_idx]
        y_tr, y_val = y_train_reset.iloc[train_idx], y_train_reset.iloc[val_idx]

        if conf["model"]["name"] == "rfr":
            model = train_rfr(model_conf, X_tr, y_tr, conf["seed"])
        else:
            model = train_xgb(model_conf, X_tr, y_tr, X_val, y_val, conf["seed"])

            bst = model.get_booster()
            log_xgb_feature_importance(run, bst, "weight")
            log_xgb_feature_importance(run, bst, "gain")
            log_xgb_feature_importance(run, bst, "cover")

        log_perm_feature_importance(run, model, X_val, y_val)

        val_preds = model.predict(X_val)
        train_preds = model.predict(X_tr)

        dummy_mean = DummyRegressor().fit(X_tr, y_tr)
        dummy_median = DummyRegressor(strategy="median").fit(X_tr, y_tr)

        val_metrics = calc_metrics(
            y_val,
            val_preds,
            dummy_mean.predict(X_val),
            dummy_median.predict(X_val),
            "val",
        )
        train_metrics = calc_metrics(
            y_tr,
            train_preds,
            dummy_mean.predict(X_tr),
            dummy_median.predict(X_tr),
            "train",
        )

        metrics = {**val_metrics, **train_metrics}

        results.append(metrics)

        run.log({f"fold_{fold}/{key}": value for key, value in metrics.items()})

    df_results = pd.DataFrame(results)

    run.log(df_results.mean().to_dict())


@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    set_seed(cfg.seed)

    df = load_dataset(version=cfg.dataset)

    with open_dict(cfg):
        cfg.dataset_start = str(df.observation_start.min().year)
        cfg.dataset_end = str(df.observation_end.max().year)

    X = df[CATEGORICAL_FEATURES + NUMERICAL_FEATURES]
    y = df[TARGET]
    groups = df["id"]

    gss = GroupShuffleSplit(n_splits=1, test_size=0.15, random_state=26)
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

        run.log(
            {
                "feat_table": wandb.Table(
                    ["features"],
                    np.expand_dims(
                        np.array([*NUMERICAL_FEATURES, *CATEGORICAL_FEATURES]), 1
                    ),
                )
            }
        )

        if conf["mode"] == "tune":
            cross_validate(run, X_train, y_train, groups_train)

        run.save(BASE_DIR / "uv.lock")


if __name__ == "__main__":
    main()
