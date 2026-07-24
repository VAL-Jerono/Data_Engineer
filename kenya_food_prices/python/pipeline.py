"""
Kenya Food Prices Data Engineering 
Module: pipeline.py — Full ETL Orchestrator

Run this directly for a one-shot pipeline execution:
    python pipeline.py
    python pipeline.py --local ../wfp_food_prices_ken.csv
    python pipeline.py --local ../wfp_food_prices_ken.csv --skip-snowflake
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from clean import clean
from extract import extract
from load import (
    ensure_database_objects,
    load_fact_table,
    load_raw,
    load_snowflake,
    load_staging,
    populate_dimensions,
)
from quality import QualityError, run_checks

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "pipeline.log", mode="a"),
    ],
)
log = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(local_path: str | None = None, skip_snowflake: bool = False) -> None:
    start = time.perf_counter()
    log.info("=" * 60)
    log.info("Kenya Food Prices ETL Pipeline — Starting")
    log.info("=" * 60)

    # ── 1. EXTRACT ─────────────────────────────────────────────
    log.info("[1/5] EXTRACT")
    raw_df = extract(local_path=local_path)
    source_name = raw_df.attrs.get("source_name")
    log.info("Extracted %d rows, %d columns.", *raw_df.shape)

    # ── 2. RAW LOAD ────────────────────────────────────────────
    log.info("[2/5] LOAD RAW")
    ensure_database_objects()
    load_raw(raw_df)

    # ── 3. TRANSFORM ───────────────────────────────────────────
    log.info("[3/5] TRANSFORM (clean)")
    clean_df = clean(raw_df)

    # ── 4. QUALITY CHECKS ──────────────────────────────────────
    log.info("[4/5] QUALITY CHECKS")
    try:
        run_checks(clean_df)
    except QualityError as exc:
        log.error("Quality gate failed — aborting pipeline: %s", exc)
        sys.exit(1)

    # ── 5. LOAD ────────────────────────────────────────────────
    log.info("[5/5] LOAD (staging → dimensions → fact)")
    if source_name:
        clean_df["source_file"] = Path(source_name).name
    load_staging(clean_df, incremental=True)
    populate_dimensions()
    load_fact_table()

    if not skip_snowflake:
        log.info("[5b] LOAD (Snowflake mirror)")
        try:
            load_snowflake(clean_df)
        except Exception as exc:
            log.warning("Snowflake load failed (non-critical): %s", exc)

    elapsed = time.perf_counter() - start
    log.info("=" * 60)
    log.info("Pipeline complete in %.1f seconds.", elapsed)
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kenya Food Prices ETL Pipeline")
    parser.add_argument("--local", help="Path to local CSV file", default=os.getenv("LOCAL_CSV_PATH"))
    parser.add_argument("--skip-snowflake", action="store_true", help="Skip Snowflake load")
    args = parser.parse_args()

    run(local_path=args.local, skip_snowflake=args.skip_snowflake)
