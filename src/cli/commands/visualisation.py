import click
from src.tools.config_loader import load_config
from src.visualisations.plots import plot_bar

@click.command()
@click.option("--config", default = "plots", help = "Config file name")
@click.option("--symbols", default = ["BTCUSDT"], help = "List name of synbol", multiple=True)
@click.option("--interval", default = "1_minute", help = "Name of the interval")
@click.option("--start_date",
    default="2025-09-01",
    help="Start date range (YYYY-MM-DD)"
)
@click.option("--end_date",
    default=None,
    help="End date range (YYYY-MM-DD)"
)
def visualisation(config: str, symbols: list[str], interval: str, start_date: str, end_date: str | None):
    config_data = load_config(config)
    plot_bar(config_data, symbols, interval, start_date, end_date)
