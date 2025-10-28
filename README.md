# TickStore Lite

TickStore Lite is a market data processing pipeline for trade-format data. It enables:

* generating Binance Futures URL lists from a configuration file,
* downloading and decompressing the corresponding trade files,
* preparing raw data in Delta Lake format using Spark,
* computing aggregated OHLCV bars over various time horizons,
* checking result consistency and producing HTML visualizations.

----
All automation is exposed via a Python CLI (python -m src.cli.main) and a Makefile designed for reproducible executions.

## Repository Contents

```
.
├── Makefile                  # Automation targets for the CLI
├── README.md                 # This document
├── configs/                  # YAML configuration files
├── requirements.txt          # Python dependencies
├── src/
│   ├── cli/                  # CLI commands (Click)
│   ├── downloader/           # Multi-threaded archive downloading
│   ├── tools/                # Configuration loading, Spark builder, utilities
│   ├── transformations/      # Pre-processing and bar computation via Spark
│   ├── url_generator/        # Binance URL generation
│   └── visualisations/       # HTML report generation (Plotly)
└── tests/                    # Demo datasets and checks
 ```

----
## Prerequisites

* Python 3.13 or higher
* Java 17+ (required for PySpark)
* Network access to download Binance archives
> **Tip**: The project uses Delta Lake. On macOS/Linux, install openjdk and set the JAVA_HOME environment variable if PySpark doesn't detect it automatically.

## Installation

1. Create a virtual environment and install dependencies:
   ```bash
   make setup
   ```
   This target creates a venv (`.venv`) and installs `requirements.txt`.

2. Activate the virtual environment:
   ```bash
   source .venv/bin/activate
   ```

3. Check the available commands:
   ```bash
   make help
   ```

## CLI Usage

All commands are run via python -m src.cli.main <command> [options]. The Makefile provides shortcuts for most scenarios.

### URL Generation

```bash
make generate GENERATE_CONFIG=url_generator GENERATE_DATE_START=2025-09-01 GENERATE_DATE_END=2025-09-07 GENERATE_OUTPUT=data/urls.json GENERATE_SYMBOLS="BTCUSDT ETHUSDT"
```

* Configuration file : `configs/url_generator.yaml`
* Modifiable parameters: symbol(s), time window, market (`--market`), output path (`--output`).
* If the **GENERATE_SYMBOLS** parameter is not specified, the symbols from **url_generator.yaml** will be used.

The output file is a JSON containing the URL and associated metadata for each day/symbol.

### Data Download

```bash
make download DOWNLOAD_INPUT=data/urls.json DOWNLOAD_OUTPUT=data/raw DOWNLOAD_WORKERS=8
```

The `src.downloader.download_data` module downloads archives in parallel, handles retries, and automatically extracts `.zip` files.

### Spark Pre-processing

```bash
make transformation-preprocess TP_INPUT=data/raw TP_OUTPUT=data/preprocessed
```

* Reads CSVs with a strict schema (`TRADE_SCHEMA`).
* Adds columns `symbol`, `date`, `market`, converts timestamp to `ts` column.
* Optional adjustment of `quote_qty` for coin-margined markets.
* Writes in partitioned Delta format.

Spark options (partitions, `maxRecordsPerFile`, write mode, sorting) are configured via `configs/preprocess.yaml`.

### OHLCV Bar Computation

```bash
make transformation-bars TB_INPUT=data/preprocessed TB_OUTPUT=data/bars TB_BAR=1m
```

* Loads Delta data.
* Aggregates by time window (`compute_ohlcv`).
* Optional gap filling (`fill_missing`) from last known values.
* Writes partitioned Delta by date/symbol (under `bucket=`).

Duration conversion (`1m`, `1h`, …) is handled by `src.tools.change_time_unit`, and Spark parameters by `configs/compute_bars.yaml`.

### Checks & Visualizations

* **Bar checks** : `make check CHECK_INPUT=tests/data/bars/1_minute CHECK_CHECK=tests/data/checks/1_minute`
* **Download reference bars** : `make download-check`
* **Plotly visualization** : `make visualisation VIS_INPUT=data/bars VIS_MODE=1_minute`

HTML files are generated in the folder defined by `configs/plots.yaml`.

### Full Integration Pipeline

To replay the entire pipeline on test fixtures:
```bash
make check-results
```
This target generates URLs, downloads test files, prepares data, computes bars, and compares the result to Binance-provided bars.

### Cleanup

* `make clear` removes main artifacts (`src/downloader/urls_list.json`, `data/`).
* `make clear-test` removes integration artifacts under `tests/data`.
* `make clear-env` removes the virtualenv.
* `make clear-all` combines the three previous commands.

## Configuration

Each module consumes a YAML file located in `configs/`. Structures are validated by Pydantic (see `src/tools/config_loader.py`). You can create a variant by duplicating an existing file and passing its name to the corresponding command:

```bash
make download DOWNLOAD_CONFIG=downloader_custom DOWNLOAD_INPUT=... DOWNLOAD_OUTPUT=...
```

Arguments passed override file values (e.g., `DOWNLOAD_WORKERS`, `TB_BAR`, etc.).

## Tests

The `tests/` folder contains:

* utility scripts (`tests/check_bars.py`, `tests/download_checks.py`) used by the CLI
* example datasets to verify pipeline functionality.
* Note: in some comparisons between Binance klines data and generated data, unexplained differences may occur, particularly on **quote_volume** and **taker_buy_quote_volume**. Even recalculating (simple sums) fails to match Binance results on certain datasets.

You can run the validation flow with make `check-results`. Adding complementary unit tests is straightforward with PyTest.

## Troubleshooting

* **PySpark won't start** : check your Java installation (`java -version`) and set `export JAVA_HOME=/chemin/vers/java`.
* **Slow downloads** : augmentez `DOWNLOAD_WORKERS` ou `DOWNLOAD_CHUNK_SIZE`.
* **Missing data in bars** : ensure filters in `configs/compute_bars.yaml` cover the desired period/symbol.
* **Empty visualization** : the Delta reader via Polars requires the folder `data/bars/<interval>` to exist and contain expected columns (`open_time`, `open`, `high`, `low`, `close`, `volume`, `vwap`).
