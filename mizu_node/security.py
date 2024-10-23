import json

from fastapi import HTTPException
from pymongo.database import Database
from redis import Redis

from mizu_node.constants import API_KEY_COLLECTION, BLOCKED_WORKER_PREFIX
from mizu_node.utils import epoch
import jwt

ALGORITHM = "EdDSA"

# ToDo: we should define a schema for the decoded payload


def verify_jwt(token: str, public_key: str) -> str:
    """verify and return user is from token, raise otherwise"""
    try:
        # Decode and validate: expiration is automatically taken care of
        payload = jwt.decode(jwt=token, key=public_key, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Token is invalid")
        return str(user_id)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token verification failed")


def verify_api_key(mdb: Database, token: str) -> str:
    doc = mdb[API_KEY_COLLECTION].find_one({"api_key": token})
    if doc is None:
        raise HTTPException(status_code=401, detail="API key is invalid")
    return doc["user"]


def block_worker(rclient: Redis, worker: str):
    rclient.set(
        BLOCKED_WORKER_PREFIX + worker,
        json.dumps({"blocked": True, "updated_at": epoch()}),
    )


def is_worker_blocked(rclient: Redis, worker: str) -> bool:
    return rclient.exists(BLOCKED_WORKER_PREFIX + worker)
