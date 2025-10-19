import requests
import json
import io
import zipfile
import gzip
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from itertools import repeat
from src.tools.config_loader import load_config


def download_and_extract(url, output_dir):
    resp = requests.get(url, stream=True)
    if resp.status_code == 200:
        if url.endswith('.zip'):
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                z.extractall(output_dir)
        elif url.endswith('.gz'):
            with gzip.open(io.BytesIO(resp.content), 'rt') as f:
                f.write(output_dir)
    return url


def execute(input, output, workers):
    with open(input, "r") as f:
        data = json.load(f)

    urls = []
    for url in data["entries"]:
        urls.append(url["url"])

    with ThreadPoolExecutor(max_workers=workers) as executor:
        list(tqdm(executor.map(download_and_extract, urls, repeat(output)), total=len(urls)))

if __name__ == "__main__":
    config = load_config("downloader")

    input = config["input"]
    output = config["output"]
    workers = config["workers"]

    execute(input, output, workers)
