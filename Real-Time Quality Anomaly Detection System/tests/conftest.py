"""
Shared pytest fixtures for unit and integration tests.
"""

import os
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from sklearn.preprocessing import StandardScaler

# ── Fixtures: feature data ────────────────────────────────────────────────────

FEATURE_COLS = [
    "deviation_count_7d",
    "deviation_count_30d",
    "complaint_rate_7d",
    "capa_overdue_ratio_30d",
    "oos_count_7d",
    "critical_ratio_7d",
    "major_ratio_7d",
    "unusual_event_count_7d",
    "material_complaint_count_7d",
    "quality_risk_open_7d",
]


@pytest.fixture
def sample_gold_df():
    """Small synthetic gold DataFrame matching gold.qa_features schema."""
    np.random.seed(42)
    n = 100
    df = pd.DataFrame({col: np.random.uniform(0, 5, n) for col in FEATURE_COLS})
    df["is_anomaly"] = np.random.choice([0, 1], n, p=[0.99, 0.01])
    return df


@pytest.fixture
def fitted_scaler(sample_gold_df):
    """StandardScaler fitted on the sample gold DataFrame."""
    scaler = StandardScaler()
    scaler.fit(sample_gold_df[FEATURE_COLS].values)
    return scaler


@pytest.fixture
def mock_iso_forest():
    """Mock Isolation Forest that returns -1 (anomaly) for the first row, 1 for rest."""
    model = MagicMock()
    model.predict.return_value = np.array([-1])
    model.decision_function.return_value = np.array([-0.15])
    return model


@pytest.fixture
def mock_autoencoder():
    """Mock Autoencoder that returns identical input (zero reconstruction error)."""
    model = MagicMock()
    model.predict.side_effect = lambda X: X  # perfect reconstruction
    return model


# ── Fixtures: Debezium messages ───────────────────────────────────────────────


@pytest.fixture
def valid_debezium_message():
    """Realistic Debezium CDC message with a valid payload."""
    return {
        "payload": {
            "after": {
                "id": 1,
                "timestamp": 1_640_000_000_000_000,
                "event_type": "deviation",
                "event_id": "EVT-001",
                "product": "Covovax",
                "system": "Filling",
                "root_cause": "Equipment failure",
                "severity": "Major",
                "batch_id": "BATCH-001",
                "status": "Closed",
                "resolution_days": 3.0,
                "closed_timestamp": 1_640_300_000_000_000,
                "is_anomaly": 0,
                "is_covid_period": 1,
                "hour": 10,
                "day_of_week": 2,
                "month": 12,
                "year": 2021,
                "is_weekend": 0,
            }
        }
    }


@pytest.fixture
def metadata_debezium_message():
    """Debezium metadata message with no 'after' field — should be skipped."""
    return {"payload": {"after": None, "op": "r", "source": {}}}


@pytest.fixture
def malformed_message():
    """Completely malformed message that should trigger a ParseError."""
    return "this is not valid json structure at all @@##"
