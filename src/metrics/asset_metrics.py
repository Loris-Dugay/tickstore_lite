import logging
from pathlib import Path
from typing import Dict, Any, Optional, List

import polars as pl

logger = logging.getLogger(__name__)


def compute_asset_metrics(config: Dict[str, Any]) -> None:
    input_path = str(Path.cwd() / config["input"])

    try:
        lf = pl.scan_delta(input_path)
    except Exception as e:
        logger.error(f"Error reading Delta Lake table at {input_path}: {e}")
        print(f"Error reading Delta Lake table at {input_path}: {e}")
        return

    symbols_filter: List[str] = config.get("symbols", [])
    start_date: Optional[str] = config.get("start_date")
    end_date: Optional[str] = config.get("end_date")

    if symbols_filter:
        lf = lf.filter(pl.col("symbol").is_in(symbols_filter))
    if start_date:
        lf = lf.filter(pl.col("date") >= pl.lit(start_date).str.to_date("%Y-%m-%d"))
    if end_date:
        lf = lf.filter(pl.col("date") <= pl.lit(end_date).str.to_date("%Y-%m-%d"))

    counts = (
        lf.group_by(["symbol", "date"])
        .agg(pl.len().alias("trade_count"))
        .sort(["symbol", "date"])
        .collect()
    )

    if counts.is_empty():
        print("No data found for the given filters.")
        return

    dates = sorted(counts["date"].unique().to_list())
    symbols = sorted(counts["symbol"].unique().to_list())

    pivot = counts.pivot(on="date", index="symbol", values="trade_count").sort("symbol")

    date_cols = [col for col in pivot.columns if col != "symbol"]
    date_cols_sorted = sorted(date_cols, key=lambda d: str(d))

    pivot = pivot.select(["symbol"] + date_cols_sorted)

    pivot = pivot.with_columns(
        pl.sum_horizontal(date_cols_sorted).alias("Total")
    )

    total_row_data: Dict[str, Any] = {"symbol": "Total"}
    for col in date_cols_sorted:
        total_row_data[col] = pivot[col].sum()
    total_row_data["Total"] = pivot["Total"].sum()

    total_row = pl.DataFrame(
        total_row_data,
        schema={col: pivot.schema[col] for col in pivot.columns},
    )

    result = pl.concat([pivot, total_row])

    _print_table(result, date_cols_sorted)


def _format_number(val: Any) -> str:
    if val is None:
        return "0"
    return f"{int(val):,}"


def _print_table(df: pl.DataFrame, date_cols: list) -> None:
    display_cols = ["symbol"] + date_cols + ["Total"]

    col_widths: Dict[str, int] = {}
    for col in display_cols:
        header_width = len(str(col))
        max_val_width = max(
            (len(_format_number(v)) for v in df[col].to_list()),
            default=0,
        )
        col_widths[col] = max(header_width, max_val_width) + 2

    header = "".join(str(col).rjust(col_widths[col]) for col in display_cols)
    separator = "-" * len(header)

    print()
    print("  Asset Metrics - Trade Count per Symbol / Day")
    print(separator)
    print(header)
    print(separator)

    for i in range(len(df)):
        row = df.row(i)
        row_dict = dict(zip(df.columns, row))

        if row_dict["symbol"] == "Total":
            print(separator)

        line = ""
        for col in display_cols:
            val = row_dict.get(col)
            if col == "symbol":
                line += str(val).rjust(col_widths[col])
            else:
                line += _format_number(val).rjust(col_widths[col])

        print(line)

    print(separator)
    print()
