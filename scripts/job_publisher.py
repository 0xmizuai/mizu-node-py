from fastapi.encoders import jsonable_encoder
import requests

from mizu_node.types import PendingJobPayload

SERVICE_URL = "http://localhost:8000"


def publish_job(job: PendingJobPayload):
    requests.post(SERVICE_URL + "/publish_jobs", json=jsonable_encoder.encode([job]))
