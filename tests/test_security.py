import jwt
import pytest
import time

from fastapi import HTTPException


SECRET_KEY = "your-secret-key"


@pytest.fixture(autouse=True)
def mock_env_var(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", SECRET_KEY)


def test_verify_jwt(monkeypatch):
    from mizu_node.security import verify_jwt

    # Test green path
    exp = time.time() + 100  # enough to not expire during this test
    token = jwt.encode(
        {"telegramUserId": 3, "exp": exp}, key=SECRET_KEY, algorithm="HS256"
    )
    user_id = verify_jwt(token)
    assert user_id == "3"

    # Now test it's expired
    exp = time.time() - 100  # already expired
    token = jwt.encode(
        {"telegramUserId": 3, "exp": exp}, key=SECRET_KEY, algorithm="HS256"
    )
    with pytest.raises(HTTPException):
        verify_jwt(token)

    # Not test missing correct user id field
    exp = time.time() + 100  # enough to not expire during this test
    token = jwt.encode(
        {"some_bad_field": 3, "exp": exp}, key=SECRET_KEY, algorithm="HS256"
    )
    with pytest.raises(HTTPException):
        verify_jwt(token)
