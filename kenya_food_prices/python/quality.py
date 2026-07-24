"""
Kenya Food Prices Data Engineering 
Module: quality.py — Data Quality Checks

Raises QualityError if any critical check fails.
All checks log results regardless; only CRITICAL checks raise.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

log = logging.getLogger("quality")


class QualityError(RuntimeError):
    """Raised when a CRITICAL quality check fails."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    level: str          # 'critical' | 'warning'
    detail: str = ""
    value: Any = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_checks(df: pd.DataFrame) -> list[CheckResult]:
    """
    Execute all quality checks against the cleaned DataFrame.

    Returns the list of CheckResult objects.
    Raises QualityError if any CRITICAL check fails.
    """
    results: list[CheckResult] = []

    results.append(_check_not_empty(df))
    results.append(_check_required_columns(df))
    results.append(_check_price_nulls(df))
    results.append(_check_date_range(df))
    results.append(_check_negative_prices(df))
    results.append(_check_duplicate_keys(df))
    results.append(_check_valid_price_ratio(df))

    _log_summary(results)

    failures = [r for r in results if not r.passed and r.level == "critical"]
    if failures:
        msgs = "; ".join(r.detail for r in failures)
        raise QualityError(f"CRITICAL quality checks failed: {msgs}")

    return results


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_not_empty(df: pd.DataFrame) -> CheckResult:
    passed = len(df) > 0
    return CheckResult(
        name="not_empty",
        passed=passed,
        level="critical",
        detail=f"DataFrame has {len(df)} rows.",
        value=len(df),
    )


def _check_required_columns(df: pd.DataFrame) -> CheckResult:
    required = {"date", "market", "commodity", "price_kes"}
    missing = required - set(df.columns)
    passed = len(missing) == 0
    return CheckResult(
        name="required_columns",
        passed=passed,
        level="critical",
        detail=f"Missing columns: {missing}" if missing else "All required columns present.",
        value=missing,
    )


def _check_price_nulls(df: pd.DataFrame) -> CheckResult:
    if "price_kes" not in df.columns:
        return CheckResult("price_nulls", False, "critical", "price_kes column missing.")
    pct_null = df["price_kes"].isna().mean() * 100
    passed = pct_null < 30  # allow up to 30% missing before flagging critical
    return CheckResult(
        name="price_nulls",
        passed=passed,
        level="critical" if pct_null >= 30 else "warning",
        detail=f"{pct_null:.1f}% of price_kes values are null.",
        value=round(pct_null, 2),
    )


def _check_date_range(df: pd.DataFrame) -> CheckResult:
    if "date" not in df.columns or df["date"].isna().all():
        return CheckResult("date_range", False, "critical", "No valid dates found.")
    min_date = df["date"].min()
    max_date = df["date"].max()
    # Expect data between 2000 and 2030
    passed = (min_date.year >= 2000) and (max_date.year <= 2030)
    return CheckResult(
        name="date_range",
        passed=passed,
        level="warning",
        detail=f"Date range: {min_date.date()} → {max_date.date()}.",
        value=(str(min_date.date()), str(max_date.date())),
    )


def _check_negative_prices(df: pd.DataFrame) -> CheckResult:
    if "price_kes" not in df.columns:
        return CheckResult("negative_prices", False, "critical", "price_kes column missing.")
    neg = (df["price_kes"] < 0).sum()
    passed = neg == 0
    return CheckResult(
        name="negative_prices",
        passed=passed,
        level="critical",
        detail=f"{neg} negative price values found.",
        value=int(neg),
    )


def _check_duplicate_keys(df: pd.DataFrame) -> CheckResult:
    key_cols = [c for c in ["date", "market", "commodity", "pricetype"] if c in df.columns]
    if not key_cols:
        return CheckResult("duplicate_keys", True, "warning", "No key columns to check.")
    dupes = df.duplicated(subset=key_cols).sum()
    passed = dupes == 0
    return CheckResult(
        name="duplicate_keys",
        passed=passed,
        level="warning",
        detail=f"{dupes} duplicate key rows found.",
        value=int(dupes),
    )


def _check_valid_price_ratio(df: pd.DataFrame) -> CheckResult:
    if "is_valid_price" not in df.columns:
        return CheckResult("valid_price_ratio", True, "warning", "is_valid_price column not present.")
    pct_valid = df["is_valid_price"].mean() * 100
    passed = pct_valid >= 70
    return CheckResult(
        name="valid_price_ratio",
        passed=passed,
        level="warning",
        detail=f"{pct_valid:.1f}% of rows have valid prices.",
        value=round(pct_valid, 2),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log_summary(results: list[CheckResult]) -> None:
    log.info("=" * 55)
    log.info("DATA QUALITY REPORT")
    log.info("=" * 55)
    for r in results:
        status = "✅ PASS" if r.passed else ("❌ FAIL" if r.level == "critical" else "⚠️  WARN")
        log.info("%s | %-25s | %s", status, r.name, r.detail)
    log.info("=" * 55)
