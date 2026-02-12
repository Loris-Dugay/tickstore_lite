import click
from src.tools.config_loader import load_config
from src.metrics.asset_metrics import compute_asset_metrics


@click.command()
@click.option("--config", default="metrics", help="Config file name")
@click.option("--input", default=None, help="Override input path (Delta Lake preprocessed data)")
@click.option("--symbols", default=None, multiple=True, help="Filter by symbol(s)")
@click.option("--start_date", default=None, help="Start date filter (YYYY-MM-DD)")
@click.option("--end_date", default=None, help="End date filter (YYYY-MM-DD)")
def metrics(config: str, input: str, symbols: tuple, start_date: str, end_date: str):
    overrides = {}
    if input is not None:
        overrides["input"] = input
    if symbols:
        overrides["symbols"] = list(symbols)
    if start_date is not None:
        overrides["start_date"] = start_date
    if end_date is not None:
        overrides["end_date"] = end_date

    config_data = load_config(config, overrides=overrides)

    compute_asset_metrics(config_data)
