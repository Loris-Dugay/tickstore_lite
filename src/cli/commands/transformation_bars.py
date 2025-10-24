import click
from src.tools.config_loader import load_config
from src.transformations.compute_bars_spark import compute_bars


@click.command()
@click.option("--config", default = "compute_bars", help = "Config file name")
@click.option("--bar_time", default = "1m", help = "Duration of the bar")
@click.option("--input", default=None, help="Override input path")
@click.option("--output", default=None, help="Override output path")
def transformation_bars(config: str, bar_time: str, input: str, output: str):
    overrides = {}
    if input is not None or output is not None:
        overrides["paths"] = {}
        if input is not None:
            overrides["paths"]["input"] = input
        if output is not None:
            overrides["paths"]["output"] = output

    config_data = load_config(config, overrides=overrides)
    compute_bars(config_data, bar_time)
