import jwt
import pytest
import time
from fastapi import HTTPException
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def test_verify_jwt():
    from mizu_node.security import verify_jwt

    # Convert to PEM format
    private_key_obj = Ed25519PrivateKey.generate()
    private_key = private_key_obj.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_key = private_key_obj.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    # Test green path
    exp = time.time() + 100  # enough to not expire during this test
    token = jwt.encode({"sub": 3, "exp": exp}, private_key, algorithm="EdDSA")
    user_id = verify_jwt(token, public_key)
    assert user_id == "3"

    # Now test it's expired
    exp = time.time() - 100  # already expired
    token = jwt.encode({"sub": 3, "exp": exp}, private_key, algorithm="EdDSA")
    with pytest.raises(HTTPException):
        verify_jwt(token, public_key)

    # Now test missing correct user id field
    exp = time.time() + 100  # enough to not expire during this test
    token = jwt.encode(
        {"some_bad_field": 3, "exp": exp}, private_key, algorithm="EdDSA"
    )
    with pytest.raises(HTTPException):
        verify_jwt(token, public_key)
