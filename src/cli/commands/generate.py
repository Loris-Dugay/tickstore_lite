import click
from datetime import date
from src.tools.config_loader import load_config
from src.url_generator.generate_urls import generate_json

@click.command()
@click.option("--config", default = "url_generator", help = "Config file name")
@click.option("--date_start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date(2025, 9, 1)),
    help="Start date range (YYYY-MM-DD)"
)
@click.option("--date_end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date(2025, 9, 1)),
    help="End date range (YYYY-MM-DD)"
)
@click.option("--symbols", default = None, help = "List name of symbols", multiple=True)
@click.option("--market", default = None, help = "Type of market (um/cm)")
@click.option("--output", default = None, help = "Path and name of the json file")

def generate(config: str,
    date_start: date,
    date_end: date,
    symbols: list[str],
    market: str,
    output: str
):
    overrides = {}
    if symbols is not None:
        overrides["symbols"] = symbols
    if output is not None:
        overrides["output_path"] = output
    if market is not None:
        overrides["market"] = market

    config_data = load_config(config, overrides)
    generate_json(config_data, date_start, date_end)
