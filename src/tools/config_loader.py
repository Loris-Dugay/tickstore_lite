import os
import logging
from pandas.core.indexes.base import Level
import yaml
from pydantic import BaseModel, ValidationError
from typing import Any, List, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, filename="logs/pipeline.log")
logger = logging.getLogger(__name__)

class ConfigError(Exception):
    pass

class UrlGeneratorConfig(BaseModel):
    symbols: list[str]
    day: int
    pattern: str
    output_path: str
    market: str

class DownloaderConfig(BaseModel):
    input: str
    output: str
    workers: int

class PreProcessConfig(BaseModel):
    input: str
    output: str
    ratio_binance: int
    numPartitions: int
    maxRecordsPerFile: int

class ComputeBars(BaseModel):
    spark: Dict[str, Any]
    paths: Dict[str, str]
    repartition_cols: List[str]
    sort_cols: List[str]
    columns: Dict[str, str]

def load_config(module: str, overrides: dict = None) -> dict:
    """Load and validate YAML config for a module."""
    config_path = os.path.join("configs", f"{module}.yaml")

    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        if config is None:
            raise ConfigError(f"Empty config file: {config_path}")

        if module == "url_generator":
            config = UrlGeneratorConfig(**config).model_dump()

        if module == "downloader":
            config = DownloaderConfig(**config).model_dump()

        if module == "preprocess":
            config = PreProcessConfig(**config).model_dump()

        if module == "compute_bars":
            config = ComputeBars(**config).model_dump()

        if overrides:
            config.update(overrides)
        logger.info(f"Loaded config for {module} from {config_path}")
        return config

    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise ConfigError(f"Config file not found: {config_path}")
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in {config_path}: {e}")
        raise ConfigError(f"Invalid YAML: {e}")
    except ValidationError as e:
        logger.error(f"Validation error in {config_path}: {e}")
        raise ConfigError(f"Invalid config: {e}")
