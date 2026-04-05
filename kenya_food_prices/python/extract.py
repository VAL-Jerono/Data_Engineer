"""
Kenya Food Prices Data Engineering Capstone
Author: Rene Bosire | Everything Data Bootcamp
Module: extract.py — Data Extraction Layer

This project is intentionally local-first because the CSV is already
available in the repo for the capstone demo flow.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("extract")

# ---------------------------------------------------------------------------
# Constants  (override via environment variables for flexibility)
# ---------------------------------------------------------------------------
DATA_URL = os.getenv(
    "FOOD_PRICES_URL",
    "https://data.humdata.org/dataset/wfp-food-prices-for-kenya/resource/"
    "0e3e8e1a-5f6e-4e1c-a1b2-3d4e5f6a7b8c/download/wfp_food_prices_ken.csv",
)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOCAL_CSV = PROJECT_ROOT / "wfp_food_prices_ken.csv"
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", PROJECT_ROOT / "data" / "raw"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract(local_path: str | None = None) -> pd.DataFrame:
    """
    Entry point for the extraction step.

    Priority order:
      1. local_path argument
      2. LOCAL_CSV_PATH environment variable
      3. repo-local default CSV
      4. Download from DATA_URL

    Returns
    -------
    pd.DataFrame
        Raw, unmodified dataframe loaded from the CSV source.
    """
    local_path = local_path or os.getenv("LOCAL_CSV_PATH")

    if local_path and Path(local_path).exists():
        log.info("Loading data from local path: %s", local_path)
        return _load_local(Path(local_path))

    if DEFAULT_LOCAL_CSV.exists():
        log.info("Loading data from default project CSV: %s", DEFAULT_LOCAL_CSV)
        return _load_local(DEFAULT_LOCAL_CSV)

    log.info("No local CSV found — downloading from URL.")
    return _download(DATA_URL)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _load_local(path: Path) -> pd.DataFrame:
    """Read a CSV from disk, returning a DataFrame."""
    try:
        df = pd.read_csv(path, low_memory=False)
        df.attrs["source_name"] = str(path.resolve())
        df.attrs["source_type"] = "local_csv"
        log.info("Loaded %d rows from %s", len(df), path)
        return df
    except Exception as exc:
        log.error("Failed to read %s: %s", path, exc)
        raise


def _download(url: str) -> pd.DataFrame:
    """
    Download the CSV dataset from the given URL.

    The raw file is saved to RAW_DATA_DIR with a datestamp so we can
    maintain a historical trail of every download.
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    datestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dest = RAW_DATA_DIR / f"wfp_food_prices_ken_{datestamp}.csv"

    log.info("Downloading dataset from %s", url)
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()

        with open(dest, "wb") as fh:
            for chunk in response.iter_content(chunk_size=8192):
                fh.write(chunk)

        log.info("Saved raw file to %s", dest)
        df = pd.read_csv(dest, low_memory=False)
        df.attrs["source_name"] = str(dest.resolve())
        df.attrs["source_type"] = "downloaded_csv"
        log.info("Downloaded %d rows", len(df))
        return df

    except requests.HTTPError as exc:
        log.error("HTTP error downloading dataset: %s", exc)
        raise
    except requests.ConnectionError as exc:
        log.error("Connection error — check URL or network: %s", exc)
        raise
    except Exception as exc:
        log.error("Unexpected extraction error: %s", exc)
        raise


# ---------------------------------------------------------------------------
# CLI convenience
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    df = extract()
    print(df.head())
    print(f"\nShape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
