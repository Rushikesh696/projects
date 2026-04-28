"""
Unit tests for ml/evaluate_model.py — prepare_features()
No database or model artifacts required.
"""
import numpy as np
import pandas as pd
import pytest

FEATURE_COLS = [
    "deviation_count_7d", "deviation_count_30d",
    "complaint_rate_7d", "capa_overdue_ratio_30d", "oos_count_7d",
    "critical_ratio_7d", "major_ratio_7d",
    "unusual_event_count_7d", "material_complaint_count_7d", "quality_risk_open_7d",
]


def prepare_features(df: pd.DataFrame, scaler):
    """Inline copy of prepare_features() for isolated testing."""
    X = df[FEATURE_COLS].fillna(0).values
    y = df["is_anomaly"].values
    X_scaled = scaler.transform(X)
    return X_scaled, y


class TestPrepareFeatures:

    def test_output_shape(self, sample_gold_df, fitted_scaler):
        X_scaled, y = prepare_features(sample_gold_df, fitted_scaler)
        assert X_scaled.shape == (len(sample_gold_df), len(FEATURE_COLS))
        assert y.shape == (len(sample_gold_df),)

    def test_correct_feature_count(self, sample_gold_df, fitted_scaler):
        X_scaled, _ = prepare_features(sample_gold_df, fitted_scaler)
        assert X_scaled.shape[1] == 10

    def test_nan_values_filled(self, sample_gold_df, fitted_scaler):
        sample_gold_df.loc[0, "deviation_count_7d"] = float("nan")
        X_scaled, _ = prepare_features(sample_gold_df, fitted_scaler)
        assert not np.isnan(X_scaled).any()

    def test_y_contains_only_binary_values(self, sample_gold_df, fitted_scaler):
        _, y = prepare_features(sample_gold_df, fitted_scaler)
        assert set(y).issubset({0, 1})

    def test_scaler_applied_mean_near_zero(self, sample_gold_df, fitted_scaler):
        X_scaled, _ = prepare_features(sample_gold_df, fitted_scaler)
        # After StandardScaler transform on training data, mean should be near 0
        assert abs(X_scaled.mean()) < 1.0

    def test_missing_feature_column_raises(self, sample_gold_df, fitted_scaler):
        df_missing = sample_gold_df.drop(columns=["deviation_count_7d"])
        with pytest.raises(KeyError):
            prepare_features(df_missing, fitted_scaler)
