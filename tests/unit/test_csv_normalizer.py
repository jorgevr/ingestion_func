"""Unit tests for src.csv_normalizer — historical CSV record normalization."""

from __future__ import annotations

from src.csv_normalizer import (
    SENSOR_SUFFIX_PATTERN,
    detect_timestamp_column,
    normalize_historical_record,
)


class TestDetectTimestampColumn:
    """detect_timestamp_column returns the first matching known column name."""

    def test_measured_on_found(self) -> None:
        assert detect_timestamp_column(["measured_on", "dc_power"]) == "measured_on"

    def test_timestamp_found(self) -> None:
        assert detect_timestamp_column(["id", "timestamp", "value"]) == "timestamp"

    def test_date_time_found(self) -> None:
        assert detect_timestamp_column(["Date-Time", "irradiance"]) == "Date-Time"

    def test_datetime_found(self) -> None:
        assert detect_timestamp_column(["datetime", "temp"]) == "datetime"

    def test_priority_order_measured_on_first(self) -> None:
        headers = ["datetime", "measured_on", "timestamp"]
        assert detect_timestamp_column(headers) == "measured_on"

    def test_no_known_column_returns_none(self) -> None:
        assert detect_timestamp_column(["id", "value", "category"]) is None

    def test_empty_headers_returns_none(self) -> None:
        assert detect_timestamp_column([]) is None


class TestSensorSuffixPattern:
    """SENSOR_SUFFIX_PATTERN strips _o_\\d+ and _\\d+ suffixes."""

    def test_strip_o_digits(self) -> None:
        assert SENSOR_SUFFIX_PATTERN.sub("", "ambient_temperature_o_149575") == "ambient_temperature"

    def test_strip_plain_digits(self) -> None:
        assert SENSOR_SUFFIX_PATTERN.sub("", "meter_revenue_grade_ac_output_meter_149578") == "meter_revenue_grade_ac_output_meter"

    def test_no_suffix_unchanged(self) -> None:
        assert SENSOR_SUFFIX_PATTERN.sub("", "ac_power") == "ac_power"

    def test_only_digits_suffix(self) -> None:
        assert SENSOR_SUFFIX_PATTERN.sub("", "poa_irradiance_o_149574") == "poa_irradiance"

    def test_complex_name_with_suffix(self) -> None:
        result = SENSOR_SUFFIX_PATTERN.sub(
            "", "combiner_dc_input_01.01.01_dc_current_string_01_(a)_o_151942"
        )
        assert result == "combiner_dc_input_01.01.01_dc_current_string_01_(a)"


class TestNormalizeHistoricalRecord:
    """normalize_historical_record produces correct output."""

    def test_valid_row_with_measured_on(self) -> None:
        row = {
            "measured_on": "2023-06-15 12:00:00",
            "ambient_temperature_o_149575": "25.3",
            "wind_speed_o_149576": "3.2",
        }
        result = normalize_historical_record(row, site_id=2107, file_name="2107_env.csv")

        assert result is not None
        assert result["SiteID"] == 2107
        assert result["measdatetime"] == "2023-06-15 12:00:00"
        assert result["ambient_temperature"] == 25.3
        assert result["wind_speed"] == 3.2

    def test_site_id_injected_not_from_csv(self) -> None:
        row = {"measured_on": "2023-01-01 00:00:00", "value": "10.5"}
        result = normalize_historical_record(row, site_id=9068, file_name="test.csv")

        assert result is not None
        assert result["SiteID"] == 9068

    def test_numeric_casting(self) -> None:
        row = {"measured_on": "2023-01-01 00:00:00", "power_123": "4500.0"}
        result = normalize_historical_record(row, site_id=2107, file_name="test.csv")

        assert result is not None
        assert result["power"] == 4500.0
        assert isinstance(result["power"], float)

    def test_non_numeric_kept_as_string(self) -> None:
        row = {"measured_on": "2023-01-01 00:00:00", "status": "active"}
        result = normalize_historical_record(row, site_id=2107, file_name="test.csv")

        assert result is not None
        assert result["status"] == "active"

    def test_empty_values_skipped(self) -> None:
        row = {"measured_on": "2023-01-01 00:00:00", "power_123": "", "temp": "25.0"}
        result = normalize_historical_record(row, site_id=2107, file_name="test.csv")

        assert result is not None
        assert "power" not in result
        assert result["temp"] == 25.0

    def test_missing_timestamp_returns_none(self) -> None:
        row = {"measured_on": "", "power": "100"}
        result = normalize_historical_record(row, site_id=2107, file_name="test.csv")

        assert result is None

    def test_no_known_timestamp_column_returns_none(self) -> None:
        row = {"id": "1", "value": "100"}
        result = normalize_historical_record(row, site_id=2107, file_name="test.csv")

        assert result is None

    def test_explicit_timestamp_column(self) -> None:
        row = {"measured_on": "2023-01-01 00:00:00", "value": "10"}
        result = normalize_historical_record(
            row, site_id=2107, file_name="test.csv", timestamp_column="measured_on"
        )

        assert result is not None
        assert result["measdatetime"] == "2023-01-01 00:00:00"

    def test_suffix_o_pattern(self) -> None:
        row = {"measured_on": "2023-01-01 00:00:00", "poa_irradiance_o_149574": "950.0"}
        result = normalize_historical_record(row, site_id=2107, file_name="test.csv")

        assert result is not None
        assert result["poa_irradiance"] == 950.0

    def test_suffix_plain_digits_pattern(self) -> None:
        row = {"measured_on": "2023-01-01 00:00:00", "meter_output_149578": "1200.5"}
        result = normalize_historical_record(row, site_id=2107, file_name="test.csv")

        assert result is not None
        assert result["meter_output"] == 1200.5
