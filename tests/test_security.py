import jwt
import pytest
import time

from fastapi import HTTPException


def test_verify_jwt(monkeypatch):
    from mizu_node.security import verify_jwt

    secret_key = "your-secret-key"

    # Test green path
    exp = time.time() + 100  # enough to not expire during this test
    token = jwt.encode(
        {"telegramUserId": 3, "exp": exp}, key=secret_key, algorithm="HS256"
    )
    user_id = verify_jwt(token, secret_key)
    assert user_id == "3"

    # Now test it's expired
    exp = time.time() - 100  # already expired
    token = jwt.encode(
        {"telegramUserId": 3, "exp": exp}, key=secret_key, algorithm="HS256"
    )
    with pytest.raises(HTTPException):
        verify_jwt(token, secret_key)

    # Not test missing correct user id field
    exp = time.time() + 100  # enough to not expire during this test
    token = jwt.encode(
        {"some_bad_field": 3, "exp": exp}, key=secret_key, algorithm="HS256"
    )
    with pytest.raises(HTTPException):
        verify_jwt(token, secret_key)
