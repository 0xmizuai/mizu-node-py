from functools import wraps
import logging
import os
import threading
import time
from typing import Iterator
from mongomock import MongoClient
from pydantic import BaseModel
import requests
from mizu_node.constants import API_KEY_COLLECTION


CC_MONGO_URL = os.environ["CC_MONGO_URL"]
CC_MONGO_DB_NAME = "commoncrawl"

MIZU_NODE_MONGO_URL = os.environ["MIZU_NODE_MONGO_URL"]
MIZU_NODE_MONGO_DB_NAME = "mizu_node"

PUBLISHED_JOBS_COLLECTION = "published_jobs"


def get_api_key(user: str):
    mclient = MongoClient(MIZU_NODE_MONGO_URL)
    api_keys = mclient[MIZU_NODE_MONGO_DB_NAME][API_KEY_COLLECTION]
    doc = api_keys.find_one({"user": user})
    if doc is None:
        raise ValueError(f"User {user} not found")
    return doc["api_key"]


def retry_with_backoff(max_retries=3, initial_delay=1, max_delay=30):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for retry in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, ValueError) as e:
                    if retry == max_retries - 1:  # Last retry
                        raise
                    wait = min(delay * (2**retry), max_delay)
                    logging.warning(
                        f"Attempt {retry + 1} failed: {str(e)}. Retrying in {wait} seconds..."
                    )
                    time.sleep(wait)
            return func(*args, **kwargs)  # Final attempt

        return wrapper

    return decorator


class DataJobPublisher(threading.Thread):
    def __init__(self, api_key: str):
        super().__init__()
        self.api_key = api_key
        self.service_url = os.environ.get("NODE_SERVICE_URL", "http://127.0.0.1:8000")

    @retry_with_backoff(max_retries=3, initial_delay=2, max_delay=30)
    def publish(self, request: BaseModel) -> Iterator[str]:
        try:
            response = requests.post(
                f"{self.service_url}{endpoint()}",
                json=request.model_dump(by_alias=True),
                headers={"Authorization": "Bearer " + self.api_key},
                timeout=30,  # Added timeout
            )
            response.raise_for_status()  # Raises HTTPError for bad status codes

            data = response.json()
            if "data" not in data or "jobIds" not in data["data"]:
                raise ValueError("Invalid response format")

            for job_id in data["data"]["jobIds"]:
                yield job_id

        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred: {str(e)}, Response: {response.text}")
            raise ValueError(f"Failed to publish jobs: {str(e)}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error occurred: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            raise ValueError(f"Failed to publish jobs: {str(e)}")

    def endpoint(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError
