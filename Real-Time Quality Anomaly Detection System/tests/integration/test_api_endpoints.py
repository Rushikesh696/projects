"""
Integration tests — FastAPI endpoints.
Requires: docker-compose up -d postgres  +  model artifacts in models/
Uses FastAPI TestClient (no live server needed).
"""
import os
import pytest
import numpy as np
from unittest.mock import MagicMock, patch
from pathlib import Path
from dotenv import load_dotenv
from fastapi.testclient import TestClient

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
os.environ.setdefault("API_SECRET_KEY", "test-secret-key-integration")
os.environ.setdefault("API_ALGORITHM", "HS256")
os.environ.setdefault("API_TOKEN_EXPIRE_MINUTES", "30")

pytestmark = pytest.mark.integration

DEMO_CREDENTIALS = {"username": "serum_qa", "password": "serum_pass123"}

SAMPLE_FEATURES = {
    "event_id": "EVT-TEST-001",
    "event_type": "deviation",
    "system": "Filling",
    "product": "Covovax",
    "severity": "Major",
    "deviation_count_7d": 8.0,
    "deviation_count_30d": 20.0,
    "complaint_rate_7d": 0.5,
    "capa_overdue_ratio_30d": 0.2,
    "oos_count_7d": 3.0,
    "critical_ratio_7d": 0.3,
    "major_ratio_7d": 0.4,
    "unusual_event_count_7d": 2.0,
    "material_complaint_count_7d": 1.0,
    "quality_risk_open_7d": 4.0,
}


@pytest.fixture(scope="module")
def mock_models():
    """Mock model artifacts so TestClient doesn't need real pkl/h5 files."""
    scaler = MagicMock()
    scaler.transform.return_value = np.zeros((1, 10))

    iso_forest = MagicMock()
    iso_forest.decision_function.return_value = np.array([-0.2])
    iso_forest.predict.return_value = np.array([-1])

    autoencoder = MagicMock()
    autoencoder.predict.return_value = np.zeros((1, 10))

    return scaler, iso_forest, autoencoder


@pytest.fixture(scope="module")
def client(mock_models):
    scaler, iso_forest, autoencoder = mock_models
    with patch("api.main.joblib") as mock_joblib, \
         patch("api.main.keras") as mock_keras:

        mock_joblib.load.side_effect = [scaler, iso_forest]
        mock_keras.models.load_model.return_value = autoencoder

        from api.main import app
        with TestClient(app) as c:
            yield c


@pytest.fixture(scope="module")
def auth_token(client):
    response = client.post("/token", data=DEMO_CREDENTIALS)
    assert response.status_code == 200
    return response.json()["access_token"]


class TestOpsEndpoints:

    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_ready_returns_200(self, client):
        response = client.get("/ready")
        assert response.status_code == 200
        assert response.json()["status"] == "ready"

    def test_ready_lists_models(self, client):
        response = client.get("/ready")
        assert "models" in response.json()
        assert "isolation_forest" in response.json()["models"]


class TestAuthEndpoints:

    def test_valid_credentials_return_token(self, client):
        response = client.post("/token", data=DEMO_CREDENTIALS)
        assert response.status_code == 200
        assert "access_token" in response.json()
        assert response.json()["token_type"] == "bearer"

    def test_invalid_password_returns_401(self, client):
        response = client.post("/token", data={"username": "serum_qa", "password": "wrong"})
        assert response.status_code == 401

    def test_invalid_username_returns_401(self, client):
        response = client.post("/token", data={"username": "hacker", "password": "serum_pass123"})
        assert response.status_code == 401


class TestPredictEndpoint:

    def test_predict_requires_auth(self, client):
        response = client.post("/v1/predict", json=SAMPLE_FEATURES)
        assert response.status_code == 401

    def test_predict_with_valid_token(self, client, auth_token):
        response = client.post(
            "/v1/predict",
            json=SAMPLE_FEATURES,
            headers={"Authorization": f"Bearer {auth_token}"},
        )
        assert response.status_code == 200

    def test_predict_response_schema(self, client, auth_token):
        response = client.post(
            "/v1/predict",
            json=SAMPLE_FEATURES,
            headers={"Authorization": f"Bearer {auth_token}"},
        )
        body = response.json()
        assert "event_id" in body
        assert "is_anomaly" in body
        assert "anomaly_score" in body
        assert "alert_triggered" in body
        assert body["is_anomaly"] in [0, 1]
        assert body["event_id"] == "EVT-TEST-001"

    def test_predict_anomaly_score_is_float(self, client, auth_token):
        response = client.post(
            "/v1/predict",
            json=SAMPLE_FEATURES,
            headers={"Authorization": f"Bearer {auth_token}"},
        )
        assert isinstance(response.json()["anomaly_score"], float)


class TestAlertsEndpoint:

    def test_alerts_requires_auth(self, client):
        response = client.get("/v1/alerts")
        assert response.status_code == 401

    def test_alerts_with_valid_token_returns_list(self, client, auth_token):
        with patch("api.routes.alerts.get_db_conn") as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.__enter__ = lambda s: s
            mock_cursor.__exit__ = MagicMock(return_value=False)
            mock_cursor.fetchall.return_value = []
            mock_conn.return_value.__enter__ = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cursor
            mock_conn.return_value.close = MagicMock()

            response = client.get(
                "/v1/alerts",
                headers={"Authorization": f"Bearer {auth_token}"},
            )
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_alerts_limit_param(self, client, auth_token):
        with patch("api.routes.alerts.get_db_conn") as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.__enter__ = lambda s: s
            mock_cursor.__exit__ = MagicMock(return_value=False)
            mock_cursor.fetchall.return_value = []
            mock_conn.return_value.cursor.return_value = mock_cursor
            mock_conn.return_value.close = MagicMock()

            response = client.get(
                "/v1/alerts?limit=10",
                headers={"Authorization": f"Bearer {auth_token}"},
            )
        assert response.status_code == 200
