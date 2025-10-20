import polars as pl
import json
import os
from datetime import datetime
from typing import List, Optional

def plot_bar(
    delta_table_path: str,
    symbols: List[str],
    interval: str,
    start_date: str,
    end_date: Optional[str]
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
                increasing: { line: { color: '#00ff00' } },
                decreasing: { line: { color: '#ff0000' } }
            };

            const volume = {
                x: data.open_time,
                y: data.volume,
                type: 'bar',
                xaxis: 'x',
                yaxis: 'y2',
                marker: { color: '#1f77b4', opacity: 0.5 }
            };

            const vwap = {
                x: data.open_time,
                y: data.vwap,
                type: 'scatter',
                mode: 'lines',
                line: { color: '#ffa500', width: 1 },
                name: 'VWAP',
                xaxis: 'x',
                yaxis: 'y'
            };

            const layout = {
                title: {
                    text: '%s',
                    x: 0.5,
                    xanchor: 'center'
                },
                xaxis: {
                    domain: [0, 1],
                    title: 'Time',
                    type: 'date',
                    tickformat: '%%H:%%M',
                    rangeslider: { visible: false }
                },
                yaxis: {
                    title: 'Price (USDT)',
                    domain: [0.3, 1]
                },
                yaxis2: {
                    title: 'Volume (%s)',
                    domain: [0, 0.25],
                    anchor: 'x'
                },
                grid: {
                    rows: 2,
                    columns: 1,
                    subplots: [['xy'], ['xy2']],
                    roworder: 'top to bottom'
                },
                showlegend: true,
                margin: { t: 50, b: 50, l: 50, r: 50 },
                height: 600
            };

            Plotly.newPlot('chart', [candlestick, volume, vwap], layout);
        </script>
    </body>
    </html>
    """

    try:
        df = pl.read_delta(delta_table_path + interval)
    except Exception as e:
        raise ValueError(f"Error reading Delta Lake table : {e}")

    output_files = []
    for symbol in symbols:
        filtered_df = df.filter(
            (pl.col("symbol") == symbol) &
            (pl.col("bucket").is_between(datetime.strptime(start_date, "%Y-%m-%d"), datetime.strptime(end_date, "%Y-%m-%d")))
        ).sort("open_time")

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

        output_file = f"{symbol.lower()}_ohlc_{start_date}_to_{end_date}.html"
        if start_date == end_date:
            output_file = f"{symbol.lower()}_ohlc_{start_date}.html"

        try:
            with open(output_file, "w") as f:
                f.write(html_template % (title, json.dumps(data), title, simplify_symbol))
            output_files.append(output_file)
            print(f"File generate : {output_file}")
        except Exception as e:
            print(f"Error generating file for : {output_file} : {e}")

    for output_file in output_files:
        os.system(f"open {output_file}")

if __name__ == "__main__":
    delta_table_path = "/Users/lorisdugay/Documents/Quant_Training/tickstore_lite/data/bars/"
    symbol = ["ETHUSDT", "BTCUSDT", "SOLUSDT"]
    interval = "1_minute"
    date = "2025-09-01"
    plot_bar(delta_table_path, symbol, interval, date, None)
