from typing import cast
from omegaconf import OmegaConf
from omegaconf.listconfig import ListConfig

from schemas.experiment_config import ExperimentConfig

def load_and_validate_config(config_filepath:str):
    dict_config = OmegaConf.load(config_filepath)

    config = cast(ExperimentConfig,OmegaConf.to_object(dict_config))
    return config


def load_and_validate_configs(config_filepath):
    dict_config = OmegaConf.load(config_filepath)
    if isinstance(dict_config, ListConfig):
        configs= [ cast(ExperimentConfig, OmegaConf.to_object(dc)) for dc in dict_config]
    else:
        configs = [cast(ExperimentConfig, OmegaConf.to_object(dict_config))]

    return configs

