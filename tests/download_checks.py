import os
import re
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.tools.change_time_unit import change_time_unit
from src.downloader.download_data import download_and_extract

logger = logging.getLogger(__name__)

def _ensure_directory(path: Path) -> None:
    """Create a directory if it does not already exist."""

    path.mkdir(parents=True, exist_ok=True)


def _create_patern_url_check(input: str, bar: str) -> list[str]:
    input_dir = Path.cwd() / input
    files = os.listdir(input_dir)
    list_url = []
    for file in files:
        symbol = file.split("-")[0]
        match_date = r'\d{4}-\d{2}-\d{2}'
        date = re.search(match_date, file)
        market = "um"
        if "USDT" not in symbol:
            symbol = symbol.split("USDT")[0] + "USD_PERP"
            market = "cm"
        pattern = f"https://data.binance.vision/data/futures/{market}/daily/klines/{symbol}/{bar}/{symbol}-{bar}-{date.group() if date else None}.zip"
        if date is not None:
            list_url.append(pattern)
    return list_url


def download_check_files(input: str,
    output: str,
    bar: str
):
    output_path = Path.cwd() / output / change_time_unit(bar)
    tmp_dir = Path.cwd() / output  / ".tmp"
    output_path_corrected = output_path.parent / output_path.name.replace(' ', '_')

    _ensure_directory(tmp_dir)
    _ensure_directory(output_path_corrected)

    list_url = _create_patern_url_check(input, bar)

    errors: list[tuple[str, str | None]] = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                download_and_extract,
                url,
                output_path_corrected,
                tmp_dir,
                1048576,
                30,
                3,
                0.5,
            )
            for url in list_url
        ]

        for future in tqdm(as_completed(futures), total=len(futures)):
            url, success, error = future.result()
            if not success:
                errors.append((url, error))

    if errors:
        failed_urls = ", ".join(url for url, _ in errors)
        logger.error("%s downloads failed: %s", len(errors), failed_urls)
        print(f"Failed to download: {len(errors)}, go to the logs to see wiches urls were not found")

if __name__ == "__main__":
    download_check_files("tests/data/raw",
        "tests/data/checks", "1m")
