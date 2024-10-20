import jwt
import os

from fastapi import HTTPException
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError


SECRET_KEY = os.environ["SECRET_KEY"]
ALGORITHM = "HS256"

# ToDo: we should define a schema for the decoded payload


def verify_jwt(token: str) -> str:
    """verify and return user is from token, raise otherwise"""
    try:
        # Decode and validate: expiration is automatically taken care of
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("telegramUserId")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Token is invalid")
        return str(user_id)
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token verification failed")
