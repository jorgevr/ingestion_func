"""CSV record normalizer for PVDAQ historical data (feature 002).

Handles the 2023-solar-data-prize CSV format which differs from the
feature 001 daily CSVs:
- No ``system_id`` column — SiteID must be injected from the S3 path
- Sensor suffix patterns: ``_o_\\d+`` and ``_\\d+`` (not ``__\\d+``)
- Timestamp column auto-detected from a priority list
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Sensor ID suffix pattern for 2023-solar-data-prize CSVs.
# Matches: _o_149575 (e.g., ambient_temperature_o_149575)
#          _149578   (e.g., meter_revenue_grade_ac_output_meter_149578)
SENSOR_SUFFIX_PATTERN = re.compile(r"(?:_o)?_\d+$")

# Ordered priority list of known timestamp column names.
_TIMESTAMP_COLUMNS = ["measured_on", "timestamp", "Date-Time", "datetime"]


def detect_timestamp_column(headers: list[str]) -> str | None:
    """Find the timestamp column from a list of CSV headers.

    Searches the priority list in order and returns the first match.

    Args:
        headers: List of CSV column header strings.

    Returns:
        The matching header name, or ``None`` if no known timestamp
        column is found.
    """
    header_set = set(headers)
    for candidate in _TIMESTAMP_COLUMNS:
        if candidate in header_set:
            return candidate
    return None


def normalize_historical_record(
    row: dict[str, str],
    site_id: int,
    file_name: str,
    timestamp_column: str | None = None,
) -> dict[str, Any] | None:
    """Normalize a single CSV row from a historical PVDAQ file.

    Injects ``SiteID`` from the caller (not from the CSV), maps the
    detected timestamp column to ``measdatetime``, strips sensor ID
    suffixes, and casts numeric values.

    Args:
        row: A single CSV row as a dict of column-name → string-value.
        site_id: PVDAQ site identifier (injected from S3 path).
        file_name: Basename of the CSV file (used for logging).
        timestamp_column: Pre-detected timestamp column name.  If
            ``None``, auto-detects from the row keys.

    Returns:
        A normalized record dict with ``SiteID`` and ``measdatetime``,
        or ``None`` if the row cannot be normalized.
    """
    # Auto-detect timestamp column if not provided
    ts_col = timestamp_column or detect_timestamp_column(list(row.keys()))
    if ts_col is None:
        logger.warning(
            "No known timestamp column found in %s — headers: %s",
            file_name,
            list(row.keys()),
        )
        return None

    measdatetime = row.get(ts_col, "").strip()
    if not measdatetime:
        logger.warning(
            "Empty timestamp in column %s for file %s",
            ts_col,
            file_name,
        )
        return None

    record: dict[str, Any] = {
        "SiteID": site_id,
        "measdatetime": measdatetime,
    }

    for key, value in row.items():
        if key == ts_col:
            continue

        clean_key = SENSOR_SUFFIX_PATTERN.sub("", key)
        stripped = value.strip() if isinstance(value, str) else value

        if stripped == "" or stripped is None:
            continue

        try:
            record[clean_key] = float(stripped)
        except (ValueError, TypeError):
            record[clean_key] = stripped

    return record
