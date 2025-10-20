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
def generate(config: str, date_start: date, date_end: date):
    config_data = load_config(config)
    generate_json(config_data, date_start, date_end)
