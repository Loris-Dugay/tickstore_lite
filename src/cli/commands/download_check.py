import click
from tests.download_checks import download_check_files

@click.command()
@click.option("--input", default="tests/data/raw", help="Override input path")
@click.option("--output", default="tests/data/checks", help="Override output path")
@click.option("--bar", default="1m", help="duration of the bar")
def download_check(input: str, output: str, bar: str):
    download_check_files(input, output, bar)
