import click
from src.tools.config_loader import load_config
from src.transformations.compute_bars_spark import compute_bars


@click.command()
@click.option("--config", default = "compute_bars", help = "Config file name")
@click.option("--bar_time", default = "1m", help = "Duration of the bar")
def transformation_bars(config: str, bar_time: str):
    config_data = load_config(config)
    compute_bars(config_data, bar_time)
