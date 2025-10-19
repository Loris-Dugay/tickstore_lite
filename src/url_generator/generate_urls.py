from src.tools.config_loader import load_config
from datetime import timedelta, date
import json


def generate_url(config: dict, date_start: date, date_end: date, market: str) -> list[str]:
    symbols = config["symbols"]
    pattern = config["pattern"]

    delta = timedelta(days=config["day"])
    entries = []
    d = date_start

    while d <= date_end:
        for symbol in symbols:
            if market == "um":
                symbol = symbol + "T"
            else:
                symbol = symbol + "_PERP"
            url = pattern.format(market=market, symbol=symbol, d=d)
            # new_url = f"https://data.binance.vision/data/futures/{market}/daily/trades/{symbol}/{symbol}-trades-{d}.zip"
            entries.append({
                "date": d.strftime("%Y-%m-%d"),
                "symbol": symbol,
                "url": url
            })
        d += delta
    return entries


def generate_json(config: dict, date_start: date, date_end: date) -> None:
    output_path: str = config["output_path"]
    market = config["market"]

    entries = generate_url(config, date_start, date_end, market)
    result = {
        "market": market,
        "start": date_start.strftime("%Y-%m-%d"),
        "end": date_end.strftime("%Y-%m-%d"),
        "entries": entries
    }
    json_str = json.dumps(result, indent=4)
    with open(output_path, "w") as f:
        f.write(json_str)


if __name__ == "__main__":
    config = load_config("url_generator")

    date_start = date(2025, 9, 1)
    date_end = date(2025, 9, 7)
    generate_json(config, date_start, date_end)
