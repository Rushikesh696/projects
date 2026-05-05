"""
Unit tests for kafka/consumer.py — parse_message()
No Kafka or PostgreSQL connection required.
"""

import pytest
from datetime import datetime

# Import directly from the module
import importlib, sys, types


def get_parse_message():
    """Import parse_message without triggering Kafka/DB connections."""
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "consumer",
        Path(__file__).resolve().parent.parent.parent / "kafka" / "consumer.py",
    )
    mod = importlib.util.module_from_spec(spec)
    # Stub out kafka and psycopg2 so the module loads without a running broker
    sys.modules.setdefault("kafka", types.ModuleType("kafka"))
    sys.modules["kafka"].KafkaConsumer = object
    sys.modules["kafka"].KafkaProducer = object
    sys.modules.setdefault("psycopg2", types.ModuleType("psycopg2"))
    sys.modules["psycopg2"].connect = lambda **kw: None
    sys.modules.setdefault("psycopg2.extras", types.ModuleType("psycopg2.extras"))
    sys.modules["psycopg2.extras"].execute_values = lambda *a, **kw: None
    spec.loader.exec_module(mod)
    return mod.parse_message


parse_message = get_parse_message()


class TestParseMessage:

    def test_valid_message_returns_row(self, valid_debezium_message):
        row, err = parse_message(valid_debezium_message)
        assert err is None
        assert row is not None
        assert row["event_id"] == "EVT-001"
        assert row["event_type"] == "deviation"
        assert row["system"] == "Filling"

    def test_valid_message_converts_timestamp(self, valid_debezium_message):
        row, err = parse_message(valid_debezium_message)
        assert err is None
        assert isinstance(row["timestamp"], datetime)
        assert isinstance(row["closed_timestamp"], datetime)

    def test_metadata_message_returns_none_no_error(self, metadata_debezium_message):
        row, err = parse_message(metadata_debezium_message)
        assert row is None
        assert err is None

    def test_malformed_message_returns_error(self, malformed_message):
        row, err = parse_message(malformed_message)
        assert row is None
        assert err is not None

    def test_missing_payload_key_handled(self):
        msg = {"not_payload": {}}
        row, err = parse_message(msg)
        # No "after" in top-level — returns (None, None) silently
        assert row is None
        assert err is None

    def test_null_optional_timestamps(self, valid_debezium_message):
        valid_debezium_message["payload"]["after"]["closed_timestamp"] = None
        row, err = parse_message(valid_debezium_message)
        assert err is None
        assert row["closed_timestamp"] is None
