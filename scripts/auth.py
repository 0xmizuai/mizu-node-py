import binascii
import os
import time
import jwt
from pymongo import MongoClient
from mizu_node.constants import MONGO_DB_NAME, MONGO_URL, API_KEY_COLLECTION
from mizu_node.security import SECRET_KEY

mclient = MongoClient(MONGO_URL)
api_keys = mclient[MONGO_DB_NAME][API_KEY_COLLECTION]


def generate_key():
    return binascii.hexlify(os.urandom(20)).decode()


def issue_api_key(publisher: str):
    key = generate_key()
    api_keys.insert_one({"api_key": key, "publisher": publisher})


def sign_jwt(user: str):
    exp = time.time() + 100  # enough to not expire during this test
    token = jwt.encode(
        {"telegramUserId": user, "exp": exp}, key=SECRET_KEY, algorithm="HS256"
    )
    return token
