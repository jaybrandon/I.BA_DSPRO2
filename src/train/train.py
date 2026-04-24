import hydra
from omegaconf import DictConfig, OmegaConf
from sklearn.ensemble import RandomForestRegressor

import wandb
from src.util import set_seed


def train_rfr(conf):
    model_conf = conf["model"]
    del model_conf['name']

    rfr = RandomForestRegressor(
        **model_conf,
        random_state=conf["seed"],
    )


def train_xgb(conf):
    pass


@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    set_seed(cfg.seed)

    with wandb.init(
        entity=cfg.wandb.entity,
        project=cfg.wandb.project,
        config=OmegaConf.to_container(cfg, resolve=True, throw_on_missing=True),  # ty:ignore[invalid-argument-type]
    ) as run:
        conf = run.config

        if conf["model"]["name"] == "rfr":
            train_rfr(conf)
        else:
            train_xgb(conf)


if __name__ == "__main__":
    main()
