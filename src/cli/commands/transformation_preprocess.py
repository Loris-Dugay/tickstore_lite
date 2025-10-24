import click
from src.tools.config_loader import load_config
from src.transformations.preprocess import preprocess

@click.command()
@click.option("--config", default = "preprocess", help = "Config file name")
@click.option("--input", default=None, help="Override input path")
@click.option("--output", default=None, help="Override output path")
def transformation_preprocess(config: str, input: str, output: str):
    overrides = {}
    if input is not None:
        overrides["input"] = input
    if output is not None:
        overrides["output"] = output

    config_data = load_config(config, overrides=overrides)

    input = config_data["input"]
    output = config_data["output"]
    ratio_binance = config_data["ratio_binance"]
    numPartitions = config_data["numPartitions"]
    maxRecordsPerFile = config_data["maxRecordsPerFile"]

    preprocess(input, output, ratio_binance, numPartitions, maxRecordsPerFile)
