"""Unit tests for mdrp_common.storage — Parquet sanitisation and key parsing."""

import io
from datetime import datetime, timezone

import pytest
import pyarrow.parquet as pq

from mdrp_common.storage import _partition_key, _extract_timestamp_from_key, _sanitise_records


class TestPartitionKey:
    def test_key_structure(self) -> None:
        ts = datetime(2024, 3, 15, 14, 30, tzinfo=timezone.utc)
        key = _partition_key("my-provider", ts, "batch-123")
        assert key == "bronze/my-provider/2024-03-15/14/events_batch-123.parquet"

    def test_key_pads_single_digit_hour(self) -> None:
        ts = datetime(2024, 1, 5, 7, 0, tzinfo=timezone.utc)
        key = _partition_key("p", ts, "b")
        assert "/07/" in key


class TestExtractTimestampFromKey:
    def test_valid_key(self) -> None:
        key = "bronze/provider-emulator/2024-03-15/14/events_abc.parquet"
        ts = _extract_timestamp_from_key(key)
        assert ts is not None
        assert ts.year == 2024
        assert ts.month == 3
        assert ts.day == 15
        assert ts.hour == 14

    def test_malformed_key_returns_none(self) -> None:
        assert _extract_timestamp_from_key("bad/key") is None

    def test_non_numeric_date_returns_none(self) -> None:
        assert _extract_timestamp_from_key("bronze/p/NOT-A-DATE/XX/f.parquet") is None


class TestSanitiseRecords:
    def test_dict_values_become_json_strings(self) -> None:
        records = [{"payload": {"price": 30.0, "tenor": "2024-03"}}]
        result = _sanitise_records(records)
        assert isinstance(result[0]["payload"], str)
        assert "30.0" in result[0]["payload"]

    def test_list_values_become_json_strings(self) -> None:
        records = [{"injected_faults": ["DELAYED", "STALE"]}]
        result = _sanitise_records(records)
        assert isinstance(result[0]["injected_faults"], str)
        assert "DELAYED" in result[0]["injected_faults"]

    def test_scalars_unchanged(self) -> None:
        records = [{"event_id": "abc", "price": 42.0, "version": 1}]
        result = _sanitise_records(records)
        assert result[0]["event_id"] == "abc"
        assert result[0]["price"] == 42.0
        assert result[0]["version"] == 1

    def test_none_values_unchanged(self) -> None:
        records = [{"field": None}]
        result = _sanitise_records(records)
        assert result[0]["field"] is None

    def test_corrupted_string_passes_through(self) -> None:
        records = [{"price": "~~CORRUPTED~~", "payload": {"x": 1}}]
        result = _sanitise_records(records)
        assert result[0]["price"] == "~~CORRUPTED~~"
        assert isinstance(result[0]["payload"], str)

    def test_parquet_roundtrip_with_mixed_types(self) -> None:
        """Sanitised records must survive a pyarrow conversion without errors."""
        import pandas as pd
        import pyarrow as pa

        records = [
            {"event_id": "a", "payload": {"price": 30.0}, "injected_faults": []},
            {"event_id": "b", "payload": {"price": "~~CORRUPTED~~"}, "injected_faults": ["MALFORMED"]},
        ]
        sanitised = _sanitise_records(records)
        df = pd.DataFrame(sanitised)
        # Should not raise
        table = pa.Table.from_pandas(df, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        assert buf.tell() > 0
