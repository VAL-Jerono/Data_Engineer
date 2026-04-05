"""
Kenya Food Prices Data Engineering Capstone
Author: Rene Bosire | Everything Data Bootcamp
Module: clean.py — Data Transformation & Cleaning Layer

Responsibilities:
  - Standardise column names
  - Parse and validate dates
  - Handle missing / invalid prices
  - Normalise market & commodity names
  - Derive price_per_kg, year, month columns
  - Return a clean DataFrame ready for loading
"""

import logging

import numpy as np
import pandas as pd

log = logging.getLogger("clean")

# ---------------------------------------------------------------------------
# Column rename map — maps whatever the CSV header is → our internal names
# ---------------------------------------------------------------------------
COLUMN_MAP = {
    "date": "date",
    "admin1": "county",
    "admin2": "district",
    "market": "market",
    "market_id": "source_market_id",
    "latitude": "latitude",
    "longitude": "longitude",
    "category": "category",
    "commodity": "commodity",
    "commodity_id": "source_commodity_id",
    "unit": "unit",
    "priceflag": "priceflag",
    "pricetype": "pricetype",
    "currency": "currency",
    "price": "price_kes",
    "usdprice": "price_usd",
}

# Units we can reliably normalise to kg
KG_CONVERSION = {
    "kg": 1.0,
    "1 kg": 1.0,
    "100 kg": 100.0,
    "90 kg": 90.0,
    "50 kg": 50.0,
    "25 kg": 25.0,
    "2 kg": 2.0,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full cleaning pipeline.

    Steps applied in order:
      1. Normalise column names
      2. Parse dates
      3. Strip & standardise string columns
      4. Handle missing and invalid prices
      5. Derive enrichment columns (year, month, price_per_kg)
      6. Flag invalid rows without dropping them (audit trail)
      7. De-duplicate

    Returns
    -------
    pd.DataFrame
        Cleaned and enriched dataframe.
    """
    log.info("Starting clean step — input shape: %s", df.shape)

    df = _rename_columns(df)
    df = _parse_dates(df)
    df = _clean_strings(df)
    df = _handle_missing_prices(df)
    df = _derive_columns(df)
    df = _flag_invalid(df)
    df = _dedup(df)

    log.info("Clean step complete — output shape: %s", df.shape)
    return df


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lower-case all headers and apply our rename map."""
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
    log.debug("Columns after rename: %s", list(df.columns))
    return df


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse the date column; rows that fail become NaT."""
    if "date" not in df.columns:
        log.warning("No 'date' column found — skipping date parsing.")
        return df

    original_nulls = df["date"].isna().sum()
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=False)
    new_nulls = df["date"].isna().sum() - original_nulls
    if new_nulls:
        log.warning("Date parsing failed for %d rows (set to NaT).", new_nulls)
    return df


def _clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace and title-case key string columns."""
    str_cols = [
        "county",
        "district",
        "market",
        "category",
        "commodity",
        "unit",
        "priceflag",
        "pricetype",
        "currency",
    ]
    for col in str_cols:
        if col not in df.columns:
            continue
        series = df[col]
        df[col] = series.where(series.notna(), pd.NA)
        df[col] = df[col].astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
        # Title-case geographic / name fields only
        if col in ("county", "district", "market", "commodity", "category"):
            df[col] = df[col].str.title()
        df[col] = df[col].replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})

    return df


def _handle_missing_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce price columns to numeric; mark zero prices as NaN
    because a zero food price is almost certainly an encoding error.
    """
    for col in ("price_kes", "price_usd"):
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
        zero_count = (df[col] == 0).sum()
        if zero_count:
            log.warning("Replacing %d zero values in '%s' with NaN.", zero_count, col)
            df[col] = df[col].replace(0, np.nan)

    return df


def _derive_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add year, month, and price_per_kg derived columns."""
    if "date" in df.columns:
        df["year"]  = df["date"].dt.year.astype("Int64")
        df["month"] = df["date"].dt.month.astype("Int64")

    for col in ("source_market_id", "source_commodity_id"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ("latitude", "longitude"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # price_per_kg — divide by unit quantity where we know the conversion
    if "price_kes" in df.columns and "unit" in df.columns:
        unit_lower = df["unit"].str.lower().str.strip()
        factor = unit_lower.map(KG_CONVERSION)
        df["price_per_kg"] = np.where(
            factor.notna() & df["price_kes"].notna(),
            df["price_kes"] / factor,
            np.nan,
        )

    return df


def _flag_invalid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a boolean 'is_valid_price' column.
    Invalid if: price is NaN, negative, or date is NaT.
    """
    price_col = "price_kes" if "price_kes" in df.columns else None
    valid = pd.Series(True, index=df.index)

    if price_col:
        valid &= df[price_col].notna()
        valid &= df[price_col] > 0

    if "date" in df.columns:
        valid &= df["date"].notna()

    df["is_valid_price"] = valid
    invalid_count = (~valid).sum()
    log.info("Flagged %d rows as invalid (is_valid_price=False).", invalid_count)
    return df


def _dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact duplicates on the natural business key."""
    key_cols = [c for c in ["date", "market", "commodity", "pricetype"] if c in df.columns]
    before = len(df)
    df = df.drop_duplicates(subset=key_cols, keep="last")
    removed = before - len(df)
    if removed:
        log.info("Removed %d duplicate rows.", removed)
    return df


# ---------------------------------------------------------------------------
# CLI convenience
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from pathlib import Path

    path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/sample.csv"
    raw = pd.read_csv(path, low_memory=False)
    cleaned = clean(raw)
    print(cleaned.head())
    print(f"\nShape: {cleaned.shape}")
    print(f"\nPrice validity counts: {cleaned['is_valid_price'].value_counts().to_dict()}")
