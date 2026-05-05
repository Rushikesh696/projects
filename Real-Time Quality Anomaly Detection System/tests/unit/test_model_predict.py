"""
Unit tests for ml/evaluate_model.py — Isolation Forest + Autoencoder evaluation logic.
Uses mock models — no real model artifacts required.
"""

import numpy as np
import pytest

# ── Inline the evaluation logic for isolated testing ─────────────────────────


def evaluate_isolation_forest(iso_forest, X_scaled, y):
    from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score

    raw_scores = iso_forest.decision_function(X_scaled)
    anomaly_scores = -raw_scores
    preds = iso_forest.predict(X_scaled)
    preds_binary = np.where(preds == -1, 1, 0)
    metrics = {
        "iso_precision": precision_score(y, preds_binary, zero_division=0),
        "iso_recall": recall_score(y, preds_binary, zero_division=0),
        "iso_f1": f1_score(y, preds_binary, zero_division=0),
        "iso_roc_auc": roc_auc_score(y, anomaly_scores),
    }
    return metrics, preds_binary, anomaly_scores


def evaluate_autoencoder(autoencoder, X_scaled, y, threshold=0.7):
    from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score

    X_reconstructed = autoencoder.predict(X_scaled)
    recon_errors = np.mean(np.power(X_scaled - X_reconstructed, 2), axis=1)
    preds_binary = (recon_errors > threshold).astype(int)
    metrics = {
        "ae_precision": precision_score(y, preds_binary, zero_division=0),
        "ae_recall": recall_score(y, preds_binary, zero_division=0),
        "ae_f1": f1_score(y, preds_binary, zero_division=0),
        "ae_roc_auc": roc_auc_score(y, recon_errors),
    }
    return metrics, preds_binary, recon_errors


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestIsolationForestEvaluation:

    def test_predict_returns_binary(self, mock_iso_forest, fitted_scaler, sample_gold_df):
        from tests.conftest import FEATURE_COLS

        X = fitted_scaler.transform(sample_gold_df[FEATURE_COLS].values[:1])
        y = np.array([1])
        mock_iso_forest.predict.return_value = np.array([-1])
        mock_iso_forest.decision_function.return_value = np.array([-0.2])
        _, preds_binary, _ = evaluate_isolation_forest(mock_iso_forest, X, y)
        assert set(preds_binary).issubset({0, 1})

    def test_anomaly_score_is_negated_decision_function(
        self, mock_iso_forest, fitted_scaler, sample_gold_df
    ):
        from tests.conftest import FEATURE_COLS

        X = fitted_scaler.transform(sample_gold_df[FEATURE_COLS].values[:1])
        y = np.array([1])
        mock_iso_forest.decision_function.return_value = np.array([-0.3])
        mock_iso_forest.predict.return_value = np.array([-1])
        _, _, anomaly_scores = evaluate_isolation_forest(mock_iso_forest, X, y)
        assert anomaly_scores[0] == pytest.approx(0.3)

    def test_if_minus1_maps_to_1(self, mock_iso_forest, fitted_scaler, sample_gold_df):
        from tests.conftest import FEATURE_COLS

        X = fitted_scaler.transform(sample_gold_df[FEATURE_COLS].values[:1])
        y = np.array([1])
        mock_iso_forest.predict.return_value = np.array([-1])
        mock_iso_forest.decision_function.return_value = np.array([-0.1])
        _, preds_binary, _ = evaluate_isolation_forest(mock_iso_forest, X, y)
        assert preds_binary[0] == 1

    def test_if_plus1_maps_to_0(self, mock_iso_forest, fitted_scaler, sample_gold_df):
        from tests.conftest import FEATURE_COLS

        X = fitted_scaler.transform(sample_gold_df[FEATURE_COLS].values[:1])
        y = np.array([0])
        mock_iso_forest.predict.return_value = np.array([1])
        mock_iso_forest.decision_function.return_value = np.array([0.1])
        _, preds_binary, _ = evaluate_isolation_forest(mock_iso_forest, X, y)
        assert preds_binary[0] == 0

    def test_metrics_keys_present(self, mock_iso_forest, fitted_scaler, sample_gold_df):
        from tests.conftest import FEATURE_COLS

        X = fitted_scaler.transform(sample_gold_df[FEATURE_COLS].values[:1])
        y = np.array([1])
        mock_iso_forest.predict.return_value = np.array([-1])
        mock_iso_forest.decision_function.return_value = np.array([-0.2])
        metrics, _, _ = evaluate_isolation_forest(mock_iso_forest, X, y)
        assert all(k in metrics for k in ["iso_precision", "iso_recall", "iso_f1", "iso_roc_auc"])


class TestAutoencoderEvaluation:

    def test_zero_reconstruction_error_no_anomalies(
        self, mock_autoencoder, fitted_scaler, sample_gold_df
    ):
        from tests.conftest import FEATURE_COLS

        X = fitted_scaler.transform(sample_gold_df[FEATURE_COLS].values)
        y = sample_gold_df["is_anomaly"].values
        mock_autoencoder.predict.side_effect = lambda x: x  # perfect reconstruction
        _, preds_binary, recon_errors = evaluate_autoencoder(mock_autoencoder, X, y, threshold=0.7)
        assert (recon_errors == 0).all()
        assert (preds_binary == 0).all()

    def test_high_reconstruction_error_triggers_anomaly(
        self, mock_autoencoder, fitted_scaler, sample_gold_df
    ):
        from tests.conftest import FEATURE_COLS

        X = fitted_scaler.transform(sample_gold_df[FEATURE_COLS].values[:1])
        y = np.array([1])
        mock_autoencoder.predict.side_effect = None
        mock_autoencoder.predict.return_value = np.ones_like(X) * 100  # huge error
        _, preds_binary, _ = evaluate_autoencoder(mock_autoencoder, X, y, threshold=0.7)
        assert preds_binary[0] == 1

    def test_recon_errors_are_non_negative(self, mock_autoencoder, fitted_scaler, sample_gold_df):
        from tests.conftest import FEATURE_COLS

        X = fitted_scaler.transform(sample_gold_df[FEATURE_COLS].values)
        y = sample_gold_df["is_anomaly"].values
        mock_autoencoder.predict.side_effect = lambda x: x + np.random.uniform(0, 0.1, x.shape)
        _, _, recon_errors = evaluate_autoencoder(mock_autoencoder, X, y, threshold=0.7)
        assert (recon_errors >= 0).all()

    def test_metrics_keys_present(self, mock_autoencoder, fitted_scaler, sample_gold_df):
        from tests.conftest import FEATURE_COLS

        X = fitted_scaler.transform(sample_gold_df[FEATURE_COLS].values)
        y = sample_gold_df["is_anomaly"].values
        mock_autoencoder.predict.side_effect = lambda x: x
        metrics, _, _ = evaluate_autoencoder(mock_autoencoder, X, y)
        assert all(k in metrics for k in ["ae_precision", "ae_recall", "ae_f1", "ae_roc_auc"])
