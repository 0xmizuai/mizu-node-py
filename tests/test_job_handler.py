import os
import time
from unittest import mock
from uuid import uuid4
from fastapi.testclient import TestClient
import pymongo
import pytest
from fastapi import status
from unittest.mock import patch as mock_patch

from mizu_node.constants import (
    API_KEY_COLLECTION,
)
from mizu_node.security import block_worker
from mizu_node.types.job import (
    ClassifyContext,
    DataJobPayload,
    JobType,
    PowContext,
    WorkerJobResult,
)
from tests.job_queue_mock import JobQueueMock
from tests.redis_mock import RedisMock
import mongomock

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from mizu_node.main import app

client = TestClient(app)

TEST_API_KEY = "test"
MOCK_MONGO_URL = "mongodb://localhost:27017"

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
# Decode bytes to string
public_key_str = public_key.decode("utf-8")


def jwt_token(user: str):
    exp = time.time() + 3600  # enough to not expire during this test
    return jwt.encode({"sub": user, "exp": exp}, private_key, algorithm="EdDSA")


@pytest.fixture()
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "MIZU_NODE_MONGO_URL": MOCK_MONGO_URL,
            "MIZU_NODE_MONGO_DB_NAME": "mizu_node",
            "VERIFY_KEY": public_key_str,
            "SHARED_SECRET": "some-secret",
            "BACKEND_SERVICE_URL": "http://localhost:3000",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield


def _build_classify_ctx(key: str):
    return ClassifyContext(
        r2_key=key,
        byte_size=1,
        checksum="0x",
    )


def _build_pow_ctx():
    return PowContext(difficulty=1, seed=str(uuid4()))


def _new_data_job_payload(job_type: str, r2_key: str) -> DataJobPayload:
    if job_type == JobType.classify:
        return DataJobPayload(
            job_type=JobType.classify, classify_ctx=_build_classify_ctx(r2_key)
        )
    else:
        return DataJobPayload(job_type=JobType.pow, pow_ctx=_build_pow_ctx())


def _publish_jobs(job_type: JobType, num_jobs=3):
    payloads = [
        _new_data_job_payload(job_type, str(i + 1)).model_dump()
        for i in range(num_jobs)
    ]
    response = client.post(
        "/publish_jobs",
        json={"data": payloads},
        headers={"Authorization": f"Bearer {TEST_API_KEY}"},
    )
    assert response.status_code == 200
    return response.json()["data"]["jobIds"]


def mock_all(mock_job_queue):
    client = pymongo.MongoClient(os.environ["MIZU_NODE_MONGO_URL"])
    mdb = client[os.environ["MIZU_NODE_MONGO_DB_NAME"]]
    app.mdb = lambda collection_name: mdb[collection_name]
    app.rclient = RedisMock()

    mdb[API_KEY_COLLECTION].insert_one({"api_key": TEST_API_KEY, "user": "test"})
    job_queues = {
        JobType.classify: JobQueueMock("classify"),
        JobType.pow: JobQueueMock("pow"),
    }
    mock_job_queue.side_effect = lambda job_type: job_queues[job_type]
    return job_queues


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("mizu_node.job_handler.job_queue")
def test_publish_jobs(mock_job_queue, setenvvar):
    job_queues = mock_all(mock_job_queue)

    # Test publishing classify jobs
    job_ids1 = _publish_jobs(JobType.classify, 3)
    assert len(job_ids1) == 3
    assert job_queues[JobType.classify].queue_len() == 3

    # Test publishing pow jobs
    job_ids2 = _publish_jobs(JobType.pow, 3)
    assert len(job_ids2) == 3
    assert job_queues[JobType.pow].queue_len() == 3


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("mizu_node.job_handler.job_queue")
def test_take_job_ok(mock_job_queue, setenvvar):
    job_queues = mock_all(mock_job_queue)

    # Publish jobs first
    pids = _publish_jobs(JobType.pow, 3)
    cids = _publish_jobs(JobType.classify, 3)

    # Take classify job 1
    worker1_jwt = jwt_token("worker1")
    response1 = client.get(
        "/take_job",
        params={"jobType": int(JobType.classify)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response1.status_code == 200
    job1 = response1.json()["data"]["job"]
    assert job1["_id"] == cids[0]
    assert job1["jobType"] == JobType.classify
    assert job_queues[JobType.classify].queue_len() == 3

    # Take pow job 1
    worker2_jwt = jwt_token("worker2")
    response2 = client.get(
        "/take_job",
        params={"jobType": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response2.status_code == 200
    job2 = response2.json()["data"]["job"]
    assert job2["_id"] == pids[0]
    assert job2["jobType"] == JobType.pow
    assert job_queues[JobType.pow].queue_len() == 3


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("mizu_node.job_handler.job_queue")
def test_take_job_error(mock_job_queue, setenvvar):
    mock_all(mock_job_queue)
    worker1_jwt = jwt_token("worker1")

    # No jobs published yet
    response = client.get(
        "/take_job",
        params={"jobType": int(JobType.classify)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200
    assert response.json()["data"]["job"] is None

    response = client.get(
        "/take_job",
        params={"jobType": int(JobType.classify)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS

    # Wrong job type (pow jobs published, trying to take classify job)
    _publish_jobs(JobType.pow, 3)
    worker2_jwt = jwt_token("worker2")
    response = client.get(
        "/take_job",
        params={"jobType": int(JobType.classify)},
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response.status_code == 200
    assert response.json()["data"]["job"] is None

    # Blocked worker
    worker3_jwt = jwt_token("worker3")
    block_worker(app.rclient, "worker3")
    response = client.get(
        "/take_job",
        params={"jobType": int(JobType.classify)},
        headers={"Authorization": f"Bearer {worker3_jwt}"},
    )
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert response.json()["message"] == "worker is blocked"


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("mizu_node.job_handler.job_queue")
@mock_patch("requests.post")
def test_finish_job_ok(mock_requests, mock_job_queue, setenvvar):
    mock_requests.return_value = None
    mock_all(mock_job_queue)

    # Publish jobs
    cids = _publish_jobs(JobType.classify, 3)
    pids = _publish_jobs(JobType.pow, 3)

    # Take jobs
    worker1_jwt = jwt_token("worker1")
    worker2_jwt = jwt_token("worker2")

    response1 = client.get(
        "/take_job",
        params={"jobType": int(JobType.classify)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response1.status_code == 200

    response2 = client.get(
        "/take_job",
        params={"jobType": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response2.status_code == 200

    # Case 1: job 1 finished by worker1
    r1 = WorkerJobResult(
        job_id=cids[0],
        job_type=JobType.classify,
        classify_result=["t1"],
    )
    response = client.post(
        "/finish_job",
        json=r1.model_dump(),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200

    # Verify job 1 in database
    j1 = app.mdb("jobs").find_one({"_id": cids[0]})
    assert j1["jobType"] == JobType.classify
    assert j1["classifyResult"] == ["t1"]
    assert j1["worker"] == "worker1"
    assert j1["finishedAt"] is not None

    # Case 2: job 2 finished by worker2
    r2 = WorkerJobResult(job_id=pids[0], job_type=JobType.pow, pow_result="0x")
    response = client.post(
        "/finish_job",
        json=r2.model_dump(),
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response.status_code == 200

    # Verify job 2 in database
    j2 = app.mdb("jobs").find_one({"_id": pids[0]})
    assert j2["jobType"] == JobType.pow
    assert j2["powResult"] == "0x"
    assert j2["worker"] == "worker2"
    assert j2["finishedAt"] is not None


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("mizu_node.job_handler.job_queue")
@mock_patch("requests.post")
def test_finish_job_error(mock_requests, mock_job_queue, setenvvar):
    mock_requests.return_value = None
    mock_all(mock_job_queue)

    # Try to finish job not in recorded - should fail
    worker1_jwt = jwt_token("worker1")
    result = WorkerJobResult(
        job_id="invalid_job_id",
        job_type=JobType.classify,
        classify_result=["t1"],
    )
    response = client.post(
        "/finish_job",
        json=result.model_dump(),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["message"] == "job not found"

    # Publish jobs
    _publish_jobs(JobType.classify, 3)

    # Take job as worker1
    response = client.get(
        "/take_job",
        params={"jobType": int(JobType.classify)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200
    job = response.json()["data"]["job"]

    # Finish job first time - should succeed
    result = WorkerJobResult(
        job_id=job["_id"],
        job_type=JobType.classify,
        classify_result=["t1"],
    )
    response = client.post(
        "/finish_job",
        json=result.model_dump(),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200

    # Try to finish same job again - should fail
    response = client.post(
        "/finish_job",
        json=result.model_dump(),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert response.json()["message"] == "job already finished"


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("mizu_node.job_handler.job_queue")
@mock_patch("requests.post")
def test_job_status(mock_requests, mock_job_queue, setenvvar):
    mock_requests.return_value = None
    mock_all(mock_job_queue)

    # Publish jobs
    cids = _publish_jobs(JobType.classify, 3)
    pids = _publish_jobs(JobType.pow, 2)
    all_job_ids = cids + pids

    # Check initial status
    response = client.post(
        "/job_status",
        json={"jobIds": all_job_ids},
        headers={"Authorization": f"Bearer {TEST_API_KEY}"},
    )
    assert response.status_code == 200
    initial_statuses = response.json()["data"]["jobs"]
    assert len(initial_statuses) == 5

    # Take and finish a classify job
    worker1_jwt = jwt_token("worker1")
    response = client.get(
        "/take_job",
        params={"jobType": int(JobType.classify)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200
    classify_job = response.json()["data"]["job"]

    result1 = WorkerJobResult(
        job_id=classify_job["_id"],
        job_type=JobType.classify,
        classify_result=["t1"],
    )
    response = client.post(
        "/finish_job",
        json=result1.model_dump(),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200

    # Take and finish a pow job
    worker2_jwt = jwt_token("worker2")
    response = client.get(
        "/take_job",
        params={"jobType": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response.status_code == 200
    pow_job = response.json()["data"]["job"]

    result2 = WorkerJobResult(
        job_id=pow_job["_id"],
        job_type=JobType.pow,
        pow_result="0x123",
    )
    response = client.post(
        "/finish_job",
        json=result2.model_dump(),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200

    # Check final status
    response = client.post(
        "/job_status",
        json={"jobIds": all_job_ids},
        headers={"Authorization": f"Bearer {TEST_API_KEY}"},
    )
    assert response.status_code == 200
    final_statuses = response.json()["data"]["jobs"]
    assert len(final_statuses) == 5

    # Count finished jobs
    finished_jobs = [s for s in final_statuses if s["finishedAt"] is not None]
    assert len(finished_jobs) == 2

    # Verify specific job statuses
    for status in final_statuses:
        if status["_id"] in [classify_job["_id"], pow_job["_id"]]:
            assert status["finishedAt"] is not None
        else:
            assert status["finishedAt"] is None
