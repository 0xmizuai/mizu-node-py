import binascii
import os
import time

import jwt
from pymongo import MongoClient
from mizu_node.constants import (
    MIZU_NODE_MONGO_DB_NAME,
    API_KEY_COLLECTION,
)
from mizu_node.security import ALGORITHM


def api_key_collection():
    MIZU_NODE_MONGO_URL = os.environ["MIZU_NODE_MONGO_URL"]
    print("Connecting to mongodb: " + MIZU_NODE_MONGO_URL)
    mclient = MongoClient(MIZU_NODE_MONGO_URL)
    return mclient[MIZU_NODE_MONGO_DB_NAME][API_KEY_COLLECTION]


def generate_key():
    return binascii.hexlify(os.urandom(20)).decode()


def issue_api_key(publisher: str):
    key = generate_key()
    api_key_collection().insert_one({"api_key": key, "user": publisher})
    return key


def get_api_keys(user: str):
    docs = api_key_collection().find({"user": user}).to_list(length=1000)
    return [doc["api_key"] for doc in docs]


def sign_jwt(user: str, private_key: str):
    headers = {
        "alg": "EdDSA",
        "typ": "JWT",
    }
    payload = {
        "exp": time.time() + 3600 * 7,
        "iat": time.time(),
        "sub": user,
    }
    return jwt.encode(payload, private_key, algorithm=ALGORITHM, headers=headers)
