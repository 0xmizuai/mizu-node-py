import jwt
import os

from fastapi import HTTPException
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError

from pymongo.database import Database

from mizu_node.constants import API_KEY_COLLECTION


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


def verify_api_key(mdb: Database, token: str) -> str:
    doc = mdb[API_KEY_COLLECTION].find_one({"api_key": token})
    if doc is None:
        raise HTTPException(status_code=401, detail="API key is invalid")
    return doc["user"]
