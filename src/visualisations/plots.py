import polars as pl
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
from src.tools.config_loader import load_config
from copy import deepcopy

def plot_bar(
    config: Dict[str, Any],
    symbols: List[str],
    interval: str,
    start_date: str,
    end_date: Optional[str] = None,
):
    if end_date is None:
        end_date = start_date

    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>%s</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    </head>
    <body>
        <div id="chart"></div>
        <script>
            const data = %s;

            const candlestick = {
                x: data.open_time,
                open: data.open,
                high: data.high,
                low: data.low,
                close: data.close,
                type: 'candlestick',
                xaxis: 'x',
                yaxis: 'y',
                increasing: { line: { color: '%s' } },
                decreasing: { line: { color: '%s' } }
            };

            const volume = {
                x: data.open_time,
                y: data.volume,
                type: 'bar',
                xaxis: 'x',
                yaxis: 'y2',
                marker: { color: '%s', opacity: %s }
            };

            const vwap = {
                x: data.open_time,
                y: data.vwap,
                type: 'scatter',
                mode: 'lines',
                line: { color: '%s', width: %s },
                name: 'VWAP',
                xaxis: 'x',
                yaxis: 'y'
            };

            const layout = %s;

            Plotly.newPlot('chart', [candlestick, volume, vwap], layout);
        </script>
    </body>
    </html>
    """

    try:
        input_dir = str(Path.cwd() / config["input"] / interval)
        df = pl.read_delta(input_dir)
    except Exception as e:
        raise ValueError(f"Error reading Delta Lake table : {e}")

    output_dir = Path.cwd() / config["output"]
    if not os.path.exists(str(output_dir)):
        os.makedirs(str(output_dir))

    output_files = []
    for symbol in symbols:
        try:
            filtered_df = df.filter(
                (pl.col("symbol") == symbol) &
                (pl.col("bucket").is_between(
                    datetime.strptime(start_date, "%Y-%m-%d"),
                    datetime.strptime(end_date, "%Y-%m-%d")
                ))
            ).sort("open_time")
        except Exception as e:
            print(f"Error filtering data for {symbol}: {e}")
            continue

        if filtered_df.is_empty():
            print(f"No data for {symbol} between {start_date} and {end_date}")
            continue

        data = {
            'open_time': filtered_df['open_time'].cast(pl.String).to_list(),
            'open': filtered_df['open'].to_list(),
            'high': filtered_df['high'].to_list(),
            'low': filtered_df['low'].to_list(),
            'close': filtered_df['close'].to_list(),
            'volume': filtered_df['volume'].to_list(),
            'vwap': filtered_df['vwap'].to_list()
        }

        simplify_symbol = symbol.split("USDT")[0]

        title = f"{symbol} OHLC - {interval} Bars ({start_date} to {end_date})"
        if start_date == end_date:
            title = f"{symbol} OHLC - {interval} Bars ({start_date})"

        layout = deepcopy(config["plotly"]["layout"])
        layout["yaxis2"]["title"] = layout["yaxis2"]["title"].format(symbol=simplify_symbol)
        layout["title"]["text"] = layout["title"]["text"].format(title=title)

        output_file = f"{symbol.lower()}_ohlc_{start_date}_to_{end_date}.html"
        if start_date == end_date:
            output_file = f"{symbol.lower()}_ohlc_{start_date}.html"

        output_path = str(output_dir / output_file)

        try:
            with open(output_path, "w") as f:
                f.write(html_template % (
                    title,
                    json.dumps(data),
                    config["plotly"]["styles"]["candlestick"]["increasing_color"],
                    config["plotly"]["styles"]["candlestick"]["decreasing_color"],
                    config["plotly"]["styles"]["volume"]["color"],
                    config["plotly"]["styles"]["volume"]["opacity"],
                    config["plotly"]["styles"]["vwap"]["color"],
                    config["plotly"]["styles"]["vwap"]["width"],
                    json.dumps(layout)
                ))
            output_files.append(output_path)
            print(f"File generate : {output_path}")
        except Exception as e:
            print(f"Error generating file for : {output_path} : {e}")

    for output_file in output_files:
        os.system(f"open {output_file}")

if __name__ == "__main__":
    delta_table_path = "/Users/lorisdugay/Documents/Quant_Training/tickstore_lite/data/bars/"
    symbol = ["ETHUSDT", "BTCUSDT", "SOLUSDT"]
    interval = "1_minute"
    date = "2025-09-01"
    config = load_config("plots")
    plot_bar(config, symbol, interval, date, None)
