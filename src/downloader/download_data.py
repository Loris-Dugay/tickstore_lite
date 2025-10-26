import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import sleep
from urllib.parse import urlparse

import requests
import zipfile
from tqdm import tqdm

logger = logging.getLogger(__name__)


def _ensure_directory(path: Path) -> None:
    """Create a directory if it does not already exist."""

    path.mkdir(parents=True, exist_ok=True)


def _resolve_filename(url: str) -> str:
    """Return a filesystem-safe filename from a URL."""

    parsed = urlparse(url)
    return Path(parsed.path).name


def _download_to_temp(
    url: str,
    tmp_dir: Path,
    chunk_size: int,
    timeout: int,
    retries: int,
    backoff: float,
) -> Path:
    """Stream a remote file to a temporary location on disk, divide by chunck to avoid too much data on the ram."""

    for attempt in range(retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as response:
                response.raise_for_status()
                suffix = Path(urlparse(url).path).suffix
                with NamedTemporaryFile(delete=False, dir=tmp_dir, suffix=suffix) as tmp_file:
                    for chunk in response.iter_content(chunk_size):
                        if chunk:
                            tmp_file.write(chunk)
                return Path(tmp_file.name)
        except Exception as exc:
            if attempt == retries:
                raise
            sleep_duration = backoff * (attempt + 1)
            logger.warning(
                "Download failed for %s (attempt %s/%s), retrying in %.1fs: %s",
                url,
                attempt + 1,
                retries + 1,
                sleep_duration,
                exc,
            )
            sleep(sleep_duration)


def _extract_archive(temp_path: Path, output_dir: Path, filename: str) -> None:
    """Extract archives (.zip/.gz) or move plain files into the output directory."""

    if filename.endswith(".zip"):
        with zipfile.ZipFile(temp_path) as archive:
            archive.extractall(output_dir)


def download_and_extract(
    url: str,
    output_dir: Path,
    tmp_dir: Path,
    chunk_size: int,
    timeout: int,
    retries: int,
    backoff: float,
) -> tuple[str, bool, str | None]:
    """Download a URL locally, extract archives and return the execution status."""

    filename = _resolve_filename(url)
    temp_path: Path | None = None

    try:
        temp_path = _download_to_temp(url, tmp_dir, chunk_size, timeout, retries, backoff)
        _extract_archive(temp_path, output_dir, filename)
        return url, True, None
    except Exception as exc:
        logger.error("Failed to download %s: %s", url, exc)
        return url, False, str(exc)
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                logger.warning("Unable to remove temporary file %s", temp_path)


def _load_urls(input_path: Path) -> list[str]:
    with open(input_path, "r") as handle:
        data = json.load(handle)

    entries = data.get("entries", [])
    return [entry["url"] for entry in entries if "url" in entry]


def execute(
    input_path: str,
    output_path: str,
    workers: int,
    chunk_size: int,
    timeout: int,
    retries: int,
    backoff: float,
) -> None:
    cwd = Path.cwd()
    input_file = cwd / input_path
    output_dir = cwd / output_path
    tmp_dir = output_dir / ".tmp"

    _ensure_directory(output_dir)
    _ensure_directory(tmp_dir)

    urls = list(_load_urls(input_file))
    if not urls:
        logger.info("No URLs to download from %s", input_file)
        return

    logger.info("Starting download of %s files into %s", len(urls), output_dir)

    errors: list[tuple[str, str | None]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                download_and_extract,
                url,
                output_dir,
                tmp_dir,
                chunk_size,
                timeout,
                retries,
                backoff,
            )
            for url in urls
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
    from src.tools.config_loader import load_config

    config = load_config("downloader")

    execute(
        config["input"],
        config["output"],
        config["workers"],
        config["chunk_size"],
        config["timeout"],
        config["retries"],
        config["backoff"],
    )
