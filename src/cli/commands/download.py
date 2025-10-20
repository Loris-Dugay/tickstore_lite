import click
from src.tools.config_loader import load_config
from src.downloader.download_data import execute

@click.command()
@click.option("--config", default = "downloader", help = "Config file name")
def download(config: str):
    config_data = load_config(config)
    input = config_data["input"]
    output = config_data["output"]
    workers = config_data["workers"]
    execute(input, output, workers)
