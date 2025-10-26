import os
import logging
import yaml
from pathlib import Path
from pydantic import BaseModel, ValidationError, Field
from typing import Any, List, Dict, Optional


logs_path = str(Path.cwd() / "logs")

if not os.path.exists(logs_path):
    os.makedirs(logs_path)

logging.basicConfig(level=logging.INFO, filename=f"{logs_path}/pipeline.log")
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
    workers: int = Field(10, gt=0)
    chunk_size: int = Field(1024 * 1024, gt=0)
    timeout: int = Field(30, gt=0)
    retries: int = Field(3, ge=0)
    backoff: float = Field(0.5, ge=0)

class PreProcessConfig(BaseModel):
    input: str
    output: str
    ratio_binance: int = Field(100, gt=0)
    numPartitions: Optional[int] = Field(default=None, gt=0)
    maxRecordsPerFile: Optional[int] = Field(default=None, gt=0)
    partition_columns: List[str] = Field(default_factory=lambda: ["symbol", "date"])
    write_mode: str = Field(default="overwrite")
    partition_overwrite_mode: Optional[str] = Field(default="dynamic")
    sort_within_partitions: bool = Field(default=False)

class ComputeBarsConfig(BaseModel):
    spark: Dict[str, Any]
    paths: Dict[str, str]
    repartition_cols: List[str]
    sort_cols: List[str]
    columns: Dict[str, str]

class Margin(BaseModel):
    top: int
    bottom: int
    left: int
    right: int

class Title(BaseModel):
    text: str  # Ex. "{title}" – sera remplacé dynamiquement
    x: float
    xanchor: str  # Ex. "center"

class XAxis(BaseModel):
    domain: List[int]
    title: str
    type: str  # Ex. "date"
    tickformat: str  # Ex. "%Y-%m-%d %H:%M"
    rangeslider: dict  # Sous-dict pour visible: bool

class YAxis(BaseModel):
    title: str
    domain: List[float]  # Ex. [0.3, 1]

class YAxis2(BaseModel):
    title: str  # Ex. "Volume ({symbol})" – sera remplacé dynamiquement
    domain: List[float]  # Ex. [0, 0.25]
    anchor: str  # Ex. "x"

class Grid(BaseModel):
    rows: int
    columns: int
    subplots: List[List[str]]  # Ex. [["xy"], ["xy2"]]
    roworder: str  # Ex. "top to bottom"

class Layout(BaseModel):
    height: int
    margin: Margin
    title: Title
    xaxis: XAxis
    yaxis: YAxis
    yaxis2: YAxis2
    grid: Grid
    showlegend: bool

class CandlestickStyle(BaseModel):
    increasing_color: str  # Ex. "#00ff00"
    decreasing_color: str  # Ex. "#ff0000"

class VolumeStyle(BaseModel):
    color: str
    opacity: float  # Ex. 0.5

class VwapStyle(BaseModel):
    color: str
    width: int

class Styles(BaseModel):
    candlestick: CandlestickStyle
    volume: VolumeStyle
    vwap: VwapStyle

class Plotly(BaseModel):
    layout: Layout
    styles: Styles

class PlotlyConfig(BaseModel):
    input: str
    output: str
    plotly: Plotly

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
            config = ComputeBarsConfig(**config).model_dump()

        if module == "plots":
            config = PlotlyConfig(**config).model_dump()

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
