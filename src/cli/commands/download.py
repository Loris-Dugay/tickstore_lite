import click
from src.tools.config_loader import load_config
from src.downloader.download_data import execute

@click.command()
@click.option("--config", default = "downloader", help = "Config file name")
@click.option("--input", default = None, help = "Path to the json file who contain all the urls")
@click.option("--output", default = None, help = "Path where the csv will be stored")
@click.option("--workers", default = None, type=int, help = "Numbers of workers used to download the csv")
@click.option("--chunk-size", default = None, type=int, help = "Chunk size (in bytes) for streamed downloads")
@click.option("--timeout", default = None, type=int, help = "Timeout in seconds for HTTP requests")
@click.option("--retries", default = None, type=int, help = "Number of retry attempts per download")
@click.option("--backoff", default = None, type=float, help = "Base backoff (seconds) between retries")
def download(config: str,
    input: str,
    output: str,
    workers: int,
    chunk_size: int,
    timeout: int,
    retries: int,
    backoff: float
):
    overrides = {}
    if input is not None:
        overrides["input"] = input
    if output is not None:
        overrides["output"] = output
    if workers is not None:
        overrides["workers"] = workers
    if chunk_size is not None:
        overrides["chunk_size"] = chunk_size
    if timeout is not None:
        overrides["timeout"] = timeout
    if retries is not None:
        overrides["retries"] = retries
    if backoff is not None:
        overrides["backoff"] = backoff

    config_data = load_config(config, overrides)

    execute(
        config_data["input"],
        config_data["output"],
        config_data["workers"],
        config_data["chunk_size"],
        config_data["timeout"],
        config_data["retries"],
        config_data["backoff"],
    )
