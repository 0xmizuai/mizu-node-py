import binascii
import os
import time

import jwt
from pymongo import MongoClient
from mizu_node.constants import MONGO_DB_NAME, MONGO_URL, API_KEY_COLLECTION
from mizu_node.security import ALGORITHM

mclient = MongoClient(MONGO_URL)
api_keys = mclient[MONGO_DB_NAME][API_KEY_COLLECTION]


def generate_key():
    return binascii.hexlify(os.urandom(20)).decode()


def issue_api_key(publisher: str):
    key = generate_key()
    api_keys.insert_one({"api_key": key, "publisher": publisher})
    return key


def get_api_keys(publisher: str):
    docs = api_keys.find({"publisher": publisher}).to_list(length=1000)
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
