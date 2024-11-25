import os
import time
from unittest import mock
from bson import ObjectId
from fastapi.testclient import TestClient
import pymongo
import pytest
from fastapi import status
from unittest.mock import patch as mock_patch

from mizu_node.constants import (
    API_KEY_COLLECTION,
    CLASSIFIER_COLLECTION,
    JOBS_COLLECTION,
)
from mizu_node.types.classifier import (
    ClassifierConfig,
    ClassifyResult,
    DataLabel,
    WetContext,
)
from mizu_node.types.data_job import (
    BatchClassifyContext,
    DataJobPayload,
    JobStatus,
    JobType,
    PowContext,
    RewardContext,
    WorkerJobResult,
)

from mizu_node.types.job_queue import job_queues
from mizu_node.types.service import (
    FinishJobRequest,
    FinishJobResponse,
    RegisterClassifierRequest,
)
from tests.redis_mock import RedisMock
import mongomock

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from mizu_node.main import app
from tests.worker_utils import block_worker, clear_cooldown, set_reward_stats

client = TestClient(app)

TEST_API_KEY1 = "test_api_key1"
TEST_API_KEY2 = "test_api_key2"
MOCK_MONGO_URL = "mongodb://localhost:27017"
CLASSIFIER_ID = ObjectId("666666666666666666666666")
MIZU_ADMIN_USER_API_KEY = "admin_key"

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


class MockedHttpResponse:
    def __init__(self, status_code, json_data={}):
        self.status_code = status_code
        self.json_data = json_data

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if self.status_code != 200:
            raise Exception()


def jwt_token(user: str):
    exp = time.time() + 3600  # enough to not expire during this test
    return jwt.encode({"sub": user, "exp": exp}, private_key, algorithm="EdDSA")


@pytest.fixture()
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "MIZU_NODE_MONGO_URL": MOCK_MONGO_URL,
            "MIZU_NODE_MONGO_DB_NAME": "mizu_node",
            "JWT_VERIFY_KEY": public_key_str,
            "API_SECRET_KEY": "some-secret",
            "BACKEND_SERVICE_URL": "http://localhost:3000",
            "MIZU_ADMIN_USER_API_KEY": MIZU_ADMIN_USER_API_KEY,
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield


def _build_pow_ctx():
    return PowContext(
        difficulty=5,
        seed="3af90f60e5705ccfa68106768481e78b08241a3f53620468b91565a9c742fd3e",
    )


def _build_batch_classify_ctx():
    return BatchClassifyContext(
        data_url="https://example.com/data",
        batch_size=1000,
        bytesize=5000000,
        decompressed_byte_size=20000000,
        checksum_md5="0x",
        classifier_id=str(CLASSIFIER_ID),
    )


def _build_reward_ctx():
    return RewardContext(amount=50)


def _new_data_job_payload(job_type: str, r2_key: str) -> DataJobPayload:
    if job_type == JobType.reward:
        return _build_reward_ctx()
    elif job_type == JobType.pow:
        return _build_pow_ctx()
    elif job_type == JobType.batch_classify:
        return _build_batch_classify_ctx()
    else:
        raise ValueError(f"Invalid job type: {job_type}")


def _publish_jobs(job_type: JobType, num_jobs=3):
    payloads = [
        _new_data_job_payload(job_type, str(i + 1)).model_dump()
        for i in range(num_jobs)
    ]

    if job_type == JobType.pow:
        endpoint = "/publish_pow_jobs"
        token = MIZU_ADMIN_USER_API_KEY
    elif job_type == JobType.batch_classify:
        endpoint = "/publish_batch_classify_jobs"
        token = TEST_API_KEY1
    elif job_type == JobType.reward:
        endpoint = "/publish_reward_jobs"
        token = MIZU_ADMIN_USER_API_KEY
    else:
        raise ValueError(f"Invalid job type: {job_type}")

    response = client.post(
        endpoint,
        json={"data": payloads},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    return response.json()["data"]["jobIds"]


def mock_all():
    client = pymongo.MongoClient(os.environ["MIZU_NODE_MONGO_URL"])
    mdb = client[os.environ["MIZU_NODE_MONGO_DB_NAME"]]
    app.mdb = mdb
    app.rclient = RedisMock()

    mdb[API_KEY_COLLECTION].insert_one({"api_key": TEST_API_KEY1, "user": "test_user1"})
    mdb[API_KEY_COLLECTION].insert_one({"api_key": TEST_API_KEY2, "user": "test_user2"})
    mdb[CLASSIFIER_COLLECTION].insert_one(
        {
            "_id": CLASSIFIER_ID,
            "publisher": "test_user1",
            "name": "default",
            "embedding_model": "test_embedding_model",
        }
    )


@mongomock.patch((MOCK_MONGO_URL))
def test_publish_jobs(setenvvar):
    # Add this line to set up the test environment
    mock_all()

    # Test publishing classify jobs
    job_ids1 = _publish_jobs(JobType.reward, 3)
    assert len(job_ids1) == 3

    # Test publishing pow jobs
    job_ids2 = _publish_jobs(JobType.pow, 3)
    assert len(job_ids2) == 3

    # Test publishing batch classify jobs
    job_ids3 = _publish_jobs(JobType.batch_classify, 3)
    assert len(job_ids3) == 3


@mongomock.patch((MOCK_MONGO_URL))
def test_take_job_ok(setenvvar):
    mock_all()

    # Publish jobs first
    pids = _publish_jobs(JobType.pow, 3)
    rids = _publish_jobs(JobType.reward, 3)
    bids = _publish_jobs(JobType.batch_classify, 3)

    # Take reward job 1
    worker1_jwt = jwt_token("worker1")
    set_reward_stats(app.rclient, "worker1")
    response1 = client.get(
        "/take_job",
        params={"job_type": int(JobType.reward)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response1.status_code == 200
    job1 = response1.json()["data"]["job"]
    assert job1["_id"] == rids[0]
    assert job1["jobType"] == JobType.reward

    # Take pow job 1
    worker2_jwt = jwt_token("worker2")
    response2 = client.get(
        "/take_job",
        params={"job_type": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response2.status_code == 200
    job2 = response2.json()["data"]["job"]
    assert job2["_id"] == pids[0]
    assert job2["jobType"] == JobType.pow

    # Take batch classify job
    worker3_jwt = jwt_token("worker3")
    response3 = client.get(
        "/take_job",
        params={"job_type": int(JobType.batch_classify)},
        headers={"Authorization": f"Bearer {worker3_jwt}"},
    )
    assert response3.status_code == 200
    job3 = response3.json()["data"]["job"]
    assert job3["_id"] == bids[0]
    assert job3["jobType"] == JobType.batch_classify


@mongomock.patch((MOCK_MONGO_URL))
def test_take_job_error(setenvvar):
    mock_all()
    worker1_jwt = jwt_token("worker1")

    # No jobs published yet
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.batch_classify)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200
    assert response.json()["data"]["job"] is None

    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.batch_classify)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS

    # Wrong job type (pow jobs published, trying to take batch classify job)
    _publish_jobs(JobType.pow, 3)
    worker2_jwt = jwt_token("worker2")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.batch_classify)},
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response.status_code == 200
    assert response.json()["data"]["job"] is None

    # Blocked worker
    worker3_jwt = jwt_token("worker3")
    block_worker(app.rclient, "worker3")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.batch_classify)},
        headers={"Authorization": f"Bearer {worker3_jwt}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert response.json()["message"] == "worker is blocked"

    # user not active for reward job
    _publish_jobs(JobType.reward, 3)
    worker4_jwt = jwt_token("worker4")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.reward)},
        headers={"Authorization": f"Bearer {worker4_jwt}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert response.json()["message"] == "not active user"

    # cooling down
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.reward)},
        headers={"Authorization": f"Bearer {worker4_jwt}"},
    )
    assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
    assert response.json()["message"].startswith("please retry after")

    # take reward job
    clear_cooldown(app.rclient, "worker4", JobType.reward)
    set_reward_stats(app.rclient, "worker4")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.reward)},
        headers={"Authorization": f"Bearer {worker4_jwt}"},
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["data"]["job"] is not None

    # should not be able to take another reward job
    clear_cooldown(app.rclient, "worker4", JobType.reward)
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.reward)},
        headers={"Authorization": f"Bearer {worker4_jwt}"},
    )
    assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
    assert response.json()["message"].startswith("please retry after")


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("requests.post")
def test_finish_job(mock_requests, setenvvar):
    mock_all()
    mock_requests.return_value = MockedHttpResponse(200)

    # Take jobs
    worker1_jwt = jwt_token("worker1")
    worker2_jwt = jwt_token("worker2")
    worker3_jwt = jwt_token("worker3")

    # Try to finish job not in recorded - should fail
    result = WorkerJobResult(
        job_id="123456781234567812345678",  # invalid job id
        job_type=JobType.pow,
        pow_result="1234",
    )
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=result).model_dump(),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["message"] == "job not found"

    # Publish jobs
    _publish_jobs(JobType.batch_classify, 3)
    _publish_jobs(JobType.pow, 3)

    response1 = client.get(
        "/take_job",
        params={"job_type": int(JobType.classify)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response1.status_code == 200
    assert response1.json()["data"]["job"] is None

    response2 = client.get(
        "/take_job",
        params={"job_type": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response2.status_code == 200
    assert response2.json()["data"]["job"] is not None
    pid = response2.json()["data"]["job"]["_id"]

    response3 = client.get(
        "/take_job",
        params={"job_type": int(JobType.batch_classify)},
        headers={"Authorization": f"Bearer {worker3_jwt}"},
    )
    assert response3.status_code == 200
    assert response3.json()["data"]["job"] is not None
    bid = response3.json()["data"]["job"]["_id"]

    # Case 1.1: finishing batch classify job with classify result
    r11 = WorkerJobResult(
        job_id=bid,
        job_type=JobType.classify,
        classify_result=["t1"],
    )
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=r11).model_dump(),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 422
    assert response.json()["message"] == "job type mismatch"

    # Case 1.2: wrong job result
    response = client.post(
        "/finish_job",
        json={
            "job_result": {
                "job_id": bid,
                "job_type": JobType.batch_classify,
                "classify_result": ["t1"],
            }
        },
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 422

    # Case 1.3: finishing batch classify job
    r13 = WorkerJobResult(
        job_id=bid,
        job_type=JobType.batch_classify,
        batch_classify_result=[
            ClassifyResult(
                labels=["t1"],
                wet_context=WetContext(warc_id="", uri="", languages=[], crawled_at=0),
            ),
            ClassifyResult(
                labels=[],
                wet_context=WetContext(warc_id="", uri="", languages=[], crawled_at=0),
            ),
        ],
    )
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=r13).model_dump(by_alias=True),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200
    assert response.json()["message"] == "ok"
    resp = FinishJobResponse.model_validate(response.json()["data"])
    assert resp.rewarded_points == 0.1

    # Verify job 1 in database
    j1 = app.mdb[JOBS_COLLECTION].find_one({"_id": ObjectId(bid)})
    assert j1["jobType"] == JobType.batch_classify
    # the empty labels are filtered out
    assert len(j1["batchClassifyResult"]) == 1
    assert j1["worker"] == "worker1"
    assert j1["finishedAt"] is not None

    # Case 1.3: finishing finished jobs
    r14 = WorkerJobResult(
        job_id=bid,
        job_type=JobType.batch_classify,
        batch_classify_result=[
            ClassifyResult(
                labels=["t1"],
                wet_context=WetContext(warc_id="", uri="", languages=[], crawled_at=0),
            ),
        ],
    )
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=r14).model_dump(by_alias=True),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 422
    assert response.json()["message"] == "job already finished"

    # Case 2: job 2 finished by worker2
    r2 = WorkerJobResult(job_id=pid, job_type=JobType.pow, pow_result="166189")
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=r2).model_dump(by_alias=True),
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response.status_code == 200
    resp = FinishJobResponse.model_validate(response.json()["data"])
    assert resp.rewarded_points == 0.1

    # Verify job 2 in database
    j2 = app.mdb[JOBS_COLLECTION].find_one({"_id": ObjectId(pid)})
    assert j2["jobType"] == JobType.pow
    assert j2["powResult"] == "166189"
    assert j2["worker"] == "worker2"
    assert j2["finishedAt"] is not None

    # Case 3: job expired
    worker4_jwt = jwt_token("worker4")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker4_jwt}"},
    )
    assert response.status_code == 200
    job_id = response.json()["data"]["job"]["_id"]

    r3 = WorkerJobResult(job_id=job_id, job_type=JobType.pow, pow_result="166189")

    # Manually expire the job by removing it from both Redis queues
    queue = job_queues[JobType.pow]
    app.rclient.delete(queue._lease_key.of(job_id))

    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=r3).model_dump(by_alias=True),
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response.status_code == 410
    assert response.json()["message"] == "job expired"


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("requests.post")
def test_job_status(mock_requests, setenvvar):
    mock_all()
    mock_requests.return_value = MockedHttpResponse(200)

    # Publish jobs
    bids = _publish_jobs(JobType.batch_classify, 3)
    pids = _publish_jobs(JobType.pow, 2)
    all_job_ids = bids + pids

    # Check initial status
    params = "&".join([f"ids={i}" for i in all_job_ids])
    response = client.get(
        f"/job_status?{params}",
        headers={"Authorization": f"Bearer {TEST_API_KEY1}"},
    )
    assert response.status_code == 200
    initial_statuses = response.json()["data"]["jobs"]
    assert len(initial_statuses) == 5

    # Take and finish a batch classify job
    worker1_jwt = jwt_token("worker1")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.batch_classify)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200
    classify_job = response.json()["data"]["job"]

    wet_context = WetContext(warc_id="", uri="", languages=[], crawled_at=0)
    classify_result = ClassifyResult(
        labels=["t1"],
        wet_context=wet_context,
    )
    result1 = WorkerJobResult(
        job_id=classify_job["_id"],
        job_type=JobType.batch_classify,
        batch_classify_result=[classify_result],
    )
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=result1).model_dump(by_alias=True),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200

    # Take and finish a pow job
    worker2_jwt = jwt_token("worker2")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response.status_code == 200
    pow_job = response.json()["data"]["job"]

    result2 = WorkerJobResult(
        job_id=pow_job["_id"], job_type=JobType.pow, pow_result="166189"
    )
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=result2).model_dump(by_alias=True),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200

    # Check final status
    response = client.get(
        f"/job_status?{params}",
        headers={"Authorization": f"Bearer {TEST_API_KEY1}"},
    )
    assert response.status_code == 200
    final_statuses = response.json()["data"]["jobs"]
    assert len(final_statuses) == 5

    # Count finished jobs
    finished_jobs = [s for s in final_statuses if s["status"] == JobStatus.finished]
    assert len(finished_jobs) == 2

    # Verify specific job statuses
    for status in final_statuses:
        if status["_id"] in [classify_job["_id"], pow_job["_id"]]:
            assert status["finishedAt"] is not None
        else:
            assert status["finishedAt"] is None


@mongomock.patch((MOCK_MONGO_URL))
def test_register_classifier(setenvvar):
    mock_all()

    classifier_config = ClassifierConfig(
        name="test_classifier",
        embedding_model="model1",
        labels=[
            DataLabel(label="0", description="0"),
            DataLabel(label="1", description="1"),
            DataLabel(label="2", description="2"),
        ],
    )
    request = RegisterClassifierRequest(config=classifier_config)

    # Try with wrong API key first
    response = client.post(
        "/register_classifier",
        json=request.model_dump(),
        headers={"Authorization": "Bearer wrong_key"},
    )
    assert response.status_code == 401

    # Try to get non-existent classifier
    response = client.get(
        "/classifier_info", params={"id": "507f1f77bcf86cd799439011"}  # Random ObjectId
    )
    assert response.status_code == 404

    # Register with correct API key
    response = client.post(
        "/register_classifier",
        json=request.model_dump(),
        headers={"Authorization": f"Bearer {TEST_API_KEY1}"},
    )
    assert response.status_code == 200
    classifier_id = response.json()["data"]["id"]
    assert classifier_id is not None

    # Get the classifier by id
    response = client.get("/classifier_info", params={"id": classifier_id})
    assert response.status_code == 200
    retrieved_classifier = response.json()["data"]["classifier"]
    assert retrieved_classifier["embeddingModel"] == classifier_config.embedding_model
    assert len(retrieved_classifier["labels"]) == len(classifier_config.labels)
    assert retrieved_classifier["publisher"] == "test_user1"

    # Try to register with mismatched publisher
    bad_config = classifier_config.model_dump()
    bad_config["name"] = "test_classifier2"
    bad_config["publisher"] = "wrong_publisher"
    response = client.post(
        "/register_classifier",
        json=RegisterClassifierRequest(config=bad_config).model_dump(),
        headers={"Authorization": f"Bearer {TEST_API_KEY2}"},
    )
    assert response.status_code == 200
    classifier_id = response.json()["data"]["id"]

    # Get the classifier by id
    response = client.get("/classifier_info", params={"id": classifier_id})
    assert response.status_code == 200
    retrieved_classifier = response.json()["data"]["classifier"]
    assert retrieved_classifier["embeddingModel"] == classifier_config.embedding_model
    assert len(retrieved_classifier["labels"]) == len(classifier_config.labels)
    assert retrieved_classifier["publisher"] == "test_user2"


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("requests.post")
def test_pow_validation(mock_requests, setenvvar):
    mock_all()
    mock_requests.return_value = MockedHttpResponse(200)

    # Publish a PoW job
    _publish_jobs(JobType.pow, 1)
    worker_jwt = jwt_token("worker1")

    # Take the job
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker_jwt}"},
    )
    assert response.status_code == 200
    job_id = response.json()["data"]["job"]["_id"]

    # Case 1: invalid pow result
    result = WorkerJobResult(
        job_id=job_id,
        job_type=JobType.pow,
        pow_result="invalid_nonce",
    )
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=result).model_dump(by_alias=True),
        headers={"Authorization": f"Bearer {worker_jwt}"},
    )
    assert response.status_code == 422
    assert (
        response.json()["message"]
        == "invalid pow_result: hash does not meet difficulty requirement"
    )

    # Case 2: valid pow result
    result = WorkerJobResult(
        job_id=job_id,
        job_type=JobType.pow,
        pow_result="166189",
    )
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=result).model_dump(by_alias=True),
        headers={"Authorization": f"Bearer {worker_jwt}"},
    )
    assert response.status_code == 200
