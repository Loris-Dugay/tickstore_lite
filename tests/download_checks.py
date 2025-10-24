import os
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from itertools import repeat
from src.tools.change_time_unit import change_time_unit
from src.downloader.download_data import download_and_extract


def download_check_files(input: str,
    output: str,
    bar: str
):
    input_dir = str(Path.cwd() / input)
    output_path = str(Path.cwd() / output / change_time_unit(bar)).replace(' ', '_')

    if not os.path.exists(output_path):
        os.makedirs(output_path)

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

    with ThreadPoolExecutor(max_workers=10) as executor:
        list(tqdm(executor.map(download_and_extract, list_url, repeat(output_path)), total=len(list_url)))


if __name__ == "__main__":
    download_check_files("tests/data/raw",
        "tests/data/checks", "1m")
