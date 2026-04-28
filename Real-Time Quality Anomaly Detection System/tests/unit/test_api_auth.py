"""
Unit tests for api/routes/auth.py — JWT token creation and verification.
No database or Kafka required.
"""
import os
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch

os.environ.setdefault("API_SECRET_KEY", "test-secret-key-for-unit-tests")
os.environ.setdefault("API_ALGORITHM", "HS256")
os.environ.setdefault("API_TOKEN_EXPIRE_MINUTES", "30")

from jose import jwt
from fastapi import HTTPException

from api.routes.auth import create_access_token, verify_token, SECRET_KEY, ALGORITHM


class TestCreateAccessToken:

    def test_returns_string(self):
        token = create_access_token({"sub": "serum_qa"})
        assert isinstance(token, str)
        assert len(token) > 0

    def test_token_contains_subject(self):
        token = create_access_token({"sub": "serum_qa"})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        assert payload["sub"] == "serum_qa"

    def test_token_contains_expiry(self):
        token = create_access_token({"sub": "serum_qa"})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        assert "exp" in payload

    def test_token_expiry_is_in_future(self):
        token = create_access_token({"sub": "serum_qa"})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        exp = datetime.utcfromtimestamp(payload["exp"])
        assert exp > datetime.utcnow()


class TestVerifyToken:

    def test_valid_token_returns_username(self):
        token = create_access_token({"sub": "serum_qa"})
        # verify_token uses Depends() — call the underlying function directly
        username = verify_token.__wrapped__(token) if hasattr(verify_token, "__wrapped__") else verify_token(token)
        assert username == "serum_qa"

    def test_invalid_token_raises_401(self):
        with pytest.raises(HTTPException) as exc_info:
            verify_token("this.is.not.a.valid.token")
        assert exc_info.value.status_code == 401

    def test_token_with_wrong_secret_raises_401(self):
        bad_token = jwt.encode({"sub": "serum_qa"}, "wrong-secret", algorithm=ALGORITHM)
        with pytest.raises(HTTPException) as exc_info:
            verify_token(bad_token)
        assert exc_info.value.status_code == 401

    def test_token_missing_sub_raises_401(self):
        token_no_sub = jwt.encode({"data": "no_subject"}, SECRET_KEY, algorithm=ALGORITHM)
        with pytest.raises(HTTPException) as exc_info:
            verify_token(token_no_sub)
        assert exc_info.value.status_code == 401
