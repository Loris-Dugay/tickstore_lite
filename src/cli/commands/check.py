import click
from tests.check_bars import check_bars


@click.command()
@click.option("--input", default=None, help="Override input path")
@click.option("--check", default=None, help="Override check path")
def check(input: str, check: str):
    if check_bars(input, check) == 0:
        print("All good, the bars generated are equals to the one given by binance")
