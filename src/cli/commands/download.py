import click
from src.tools.config_loader import load_config
from src.downloader.download_data import execute

@click.command()
@click.option("--config", default = "downloader", help = "Config file name")
@click.option("--input", default = None, help = "Path to the json file who contain all the urls")
@click.option("--output", default = None, help = "Path where the csv will be stored")
@click.option("--workers", default = None, help = "Numbers of workers used to download the csv")
def download(config: str,
    input: str,
    output: str,
    workers: int
):
    overrides = {}
    if input is not None:
        overrides["input"] = input
    if output is not None:
        overrides["output"] = output
    if workers is not None:
        overrides["workers"] = workers


    config_data = load_config(config, overrides)

    input = config_data["input"]
    output = config_data["output"]
    workers = config_data["workers"]

    execute(input, output, workers)
