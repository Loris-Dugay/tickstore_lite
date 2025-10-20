import click
from src.tools.config_loader import load_config
from src.transformations.preprocess import preprocess

@click.command()
@click.option("--config", default = "preprocess", help = "Config file name")
def transformation_preprocess(config: str):
    config_data = load_config(config)

    input = config_data["input"]
    output = config_data["output"]
    ratio_binance = config_data["ratio_binance"]
    numPartitions = config_data["numPartitions"]
    maxRecordsPerFile = config_data["maxRecordsPerFile"]

    preprocess(input, output, ratio_binance, numPartitions, maxRecordsPerFile)
