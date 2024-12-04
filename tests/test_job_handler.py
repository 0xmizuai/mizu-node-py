from contextlib import closing
import datetime
import os
from unittest import mock
import testing.postgresql
from fastapi.testclient import TestClient
import psycopg2
import pytest
from fastapi import status
from unittest.mock import MagicMock, patch as mock_patch

from mizu_node.db.classifier import store_config
from mizu_node.common import epoch

from mizu_node.db.api_key import create_api_key
from mizu_node.db.common import initiate_db
from mizu_node.db.job_queue import delete_one_job
from mizu_node.db.job_queue import get_jobs_info
from mizu_node.stats import get_valid_rewards
from mizu_node.types.classifier import (
    ClassifierConfig,
    ClassifyResult,
    DataLabel,
    WetContext,
)
from mizu_node.types.data_job import (
    BatchClassifyContext,
    JobStatus,
    JobType,
    PowContext,
    RewardContext,
    RewardJobRecords,
    RewardResult,
    Token,
    WorkerJobResult,
)

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

from tests.worker_utils import (
    block_worker,
    clear_cooldown,
    set_reward_stats,
)
from freezegun import freeze_time

from unittest import mock
import pytest

TEST_API_KEY1 = "test_api_key1"
TEST_API_KEY2 = "test_api_key2"
MOCK_MONGO_URL = "mongodb://localhost:27017"
MIZU_ADMIN_USER_API_KEY = "admin_key"


# Mock the Connections class
class MockConnections:
    def __init__(self, mdb=None, postgres=None, redis=None):
        self.mdb = mdb if mdb is not None else MagicMock()
        self.postgres = postgres if postgres is not None else MagicMock()
        self.redis = redis if redis is not None else RedisMock()


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


@pytest.fixture
def pg_conn():
    with testing.postgresql.Postgresql() as postgresql:
        # Create a connection to the temporary database
        conn = psycopg2.connect(**postgresql.dsn())
        yield conn
        # Cleanup
        conn.close()


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
    exp = epoch() + 3600  # enough to not expire during this test
    return jwt.encode({"sub": user, "exp": exp}, private_key, algorithm="EdDSA")


@pytest.fixture()
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "MIZU_NODE_MONGO_URL": MOCK_MONGO_URL,
            "POSTGRES_URL": "postgresql://postgres:postgres@localhost:5432/postgres",
            "REDIS_URL": "redis://localhost:6379",
            "MIZU_NODE_MONGO_DB_NAME": "mizu_node",
            "JWT_VERIFY_KEY": public_key_str,
            "API_SECRET_KEY": "some-secret",
            "BACKEND_SERVICE_URL": "http://localhost:3000",
            "MIZU_ADMIN_USER_API_KEY": MIZU_ADMIN_USER_API_KEY,
            "ACTIVE_USER_PAST_7D_THRESHOLD": "50",
            "MIN_REWARD_GAP": "1800",
            "ENABLE_ACTIVE_USER_CHECK": "true",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield


def _build_pow_ctx():
    return PowContext(
        difficulty=5,
        seed="3af90f60e5705ccfa68106768481e78b08241a3f53620468b91565a9c742fd3e",
    )


def _build_batch_classify_ctx(config_id: int):
    return BatchClassifyContext(
        data_url="https://example.com/data",
        batch_size=1000,
        bytesize=5000000,
        decompressed_byte_size=20000000,
        checksum_md5="0x",
        classifier_id=config_id,
    )


def _build_reward_ctx(token: Token | None = None):
    return RewardContext(token=token, amount=50)


def _new_data_job_payload(
    job_type: str,
    config_id: int | None = None,
) -> RewardContext | PowContext | BatchClassifyContext:
    if job_type == JobType.reward:
        return _build_reward_ctx()
    elif job_type == JobType.pow:
        return _build_pow_ctx()
    elif job_type == JobType.batch_classify:
        return _build_batch_classify_ctx(config_id)
    else:
        raise ValueError(f"Invalid job type: {job_type}")


def _publish_jobs(
    client: TestClient,
    job_type: JobType,
    payloads: list[RewardContext] | list[PowContext] | list[BatchClassifyContext],
):
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
        json={"data": [p.model_dump() for p in payloads]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    return response.json()["data"]["jobIds"]


def _publish_jobs_simple(
    client: TestClient, job_type: JobType, num_jobs=3, config_id: int | None = None
):
    payloads = [_new_data_job_payload(job_type, config_id) for i in range(num_jobs)]
    return _publish_jobs(client, job_type, payloads)


@pytest.fixture(scope="function")
def mock_connections(monkeypatch, pg_conn, setenvvar):
    monkeypatch.setattr("mizu_node.types.connections.Connections", MockConnections)
    mdb = mongomock.MongoClient(MOCK_MONGO_URL)[os.environ["MIZU_NODE_MONGO_DB_NAME"]]

    # Create mock connections
    mock_conn = MockConnections(mdb=mdb, postgres=pg_conn, redis=RedisMock())

    # Mock the global conn instance in main.py
    import mizu_node.main

    monkeypatch.setattr("mizu_node.main.conn", mock_conn)

    from mizu_node.main import app

    app.state.conn = mock_conn

    initiate_db(mock_conn.postgres)
    create_api_key(pg_conn, "test_user1", TEST_API_KEY1)
    create_api_key(pg_conn, "test_user2", TEST_API_KEY2)
    classififer_id = store_config(
        pg_conn,
        ClassifierConfig(
            name="test_classifier",
            embedding_model="test_embedding_model",
            labels=[DataLabel(label="t1", description="test_label1")],
            publisher="test_user1",
        ),
    )
    app.state.classifier_id = classififer_id
    return app


@mongomock.patch((MOCK_MONGO_URL))
def test_publish_jobs_simple(mock_connections):
    client = TestClient(mock_connections)
    classifier_id = client.app.state.classifier_id

    # Test publishing classify jobs
    job_ids1 = _publish_jobs_simple(client, JobType.reward, 3)
    assert len(job_ids1) == 3

    # Test publishing pow jobs
    job_ids2 = _publish_jobs_simple(client, JobType.pow, 3)
    assert len(job_ids2) == 3

    # Test publishing batch classify jobs
    job_ids3 = _publish_jobs_simple(client, JobType.batch_classify, 3, classifier_id)
    assert len(job_ids3) == 3


@mongomock.patch((MOCK_MONGO_URL))
def test_take_job_ok(mock_connections):
    client = TestClient(mock_connections)
    classifier_id = client.app.state.classifier_id

    # Publish jobs first
    pids = _publish_jobs_simple(client, JobType.pow, 3)
    rids = _publish_jobs_simple(client, JobType.reward, 3)
    bids = _publish_jobs_simple(client, JobType.batch_classify, 3, classifier_id)

    # Take reward job 1
    worker1_jwt = jwt_token("worker1")
    set_reward_stats(mock_connections.state.conn.redis, "worker1")
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
def test_take_job_error(mock_connections):
    client = TestClient(mock_connections)
    worker1_jwt = jwt_token("worker1")

    # No jobs published yet
    for _ in range(10):
        response = client.get(
            "/take_job",
            params={"job_type": int(JobType.pow)},
            headers={"Authorization": f"Bearer {worker1_jwt}"},
        )
        assert response.status_code == 200
        assert response.json()["data"].get("job", None) is None

    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS

    # Wrong job type (pow jobs published, trying to take batch classify job)
    _publish_jobs_simple(client, JobType.pow, 3)
    worker2_jwt = jwt_token("worker2")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.batch_classify)},
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response.status_code == 200
    assert response.json()["data"].get("job", None) is None

    # Blocked worker
    worker3_jwt = jwt_token("worker3")
    block_worker(mock_connections.state.conn.redis, "worker3")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.batch_classify)},
        headers={"Authorization": f"Bearer {worker3_jwt}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert response.json()["message"] == "worker is blocked"

    # user not active for reward job
    _publish_jobs_simple(client, JobType.reward, 3)
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
    clear_cooldown(mock_connections.state.conn.redis, "worker4", JobType.reward)
    set_reward_stats(mock_connections.state.conn.redis, "worker4")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.reward)},
        headers={"Authorization": f"Bearer {worker4_jwt}"},
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["data"]["job"] is not None

    # should not be able to take another reward job
    clear_cooldown(mock_connections.state.conn.redis, "worker4", JobType.reward)
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.reward)},
        headers={"Authorization": f"Bearer {worker4_jwt}"},
    )
    assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
    assert response.json()["message"].startswith("please retry after")


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("requests.post")
def test_finish_job(mock_requests, mock_connections):
    client = TestClient(mock_connections)
    classifier_id = client.app.state.classifier_id

    mock_requests.return_value = MockedHttpResponse(200)

    # Take jobs
    worker1_jwt = jwt_token("worker1")
    worker2_jwt = jwt_token("worker2")
    worker3_jwt = jwt_token("worker3")

    # Publish jobs
    _publish_jobs_simple(client, JobType.reward, 3)
    _publish_jobs_simple(client, JobType.batch_classify, 3, classifier_id)
    _publish_jobs_simple(client, JobType.pow, 3)

    response1 = client.get(
        "/take_job",
        params={"job_type": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response1.status_code == 200
    assert response1.json()["data"]["job"] is not None
    pid = response1.json()["data"]["job"]["_id"]

    # delete pow job 1 from database
    delete_one_job(mock_connections.state.conn.postgres, pid)
    # Try to finish job not in recorded - should fail
    result = WorkerJobResult(
        job_id=pid,  # job id not exists
        job_type=JobType.pow,
        pow_result="1234",
    )
    response2 = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=result).model_dump(),
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response2.status_code == status.HTTP_404_NOT_FOUND
    assert response2.json()["message"] == "lease not exists"

    response2 = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=result).model_dump(),
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response2.status_code == status.HTTP_404_NOT_FOUND

    response3 = client.get(
        "/take_job",
        params={"job_type": int(JobType.batch_classify)},
        headers={"Authorization": f"Bearer {worker3_jwt}"},
    )
    assert response3.status_code == 200
    assert response3.json()["data"]["job"] is not None
    bid = response3.json()["data"]["job"]["_id"]

    # Case 1.1: finishing batch classify job with classify result
    response = client.post(
        "/finish_job",
        json={
            "jobResult": {
                "jobId": bid,
                "jobType": JobType.batch_classify,
                "classifyResult": ["t1"],
            }
        },
        headers={"Authorization": f"Bearer {worker3_jwt}"},
    )
    assert response.status_code == 422

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
        headers={"Authorization": f"Bearer {worker3_jwt}"},
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
        headers={"Authorization": f"Bearer {worker3_jwt}"},
    )
    assert response.status_code == 200
    assert response.json()["message"] == "ok"
    resp = FinishJobResponse.model_validate(response.json()["data"])
    assert resp.rewarded_points == 0.5

    # Verify job 1 in database
    j1 = get_jobs_info(mock_connections.state.conn.postgres, [bid])[0]
    assert j1.job_type == JobType.batch_classify
    # the empty labels are filtered out
    assert len(j1.result.batch_classify_result) == 1
    assert j1.worker == "worker3"
    assert j1.finished_at is not None

    # Case 1.4: finishing finished jobs
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
        headers={"Authorization": f"Bearer {worker3_jwt}"},
    )
    assert response.status_code == 404

    # Case 2: job 2 finished by worker2
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.pow)},
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response.status_code == 200
    assert response.json()["data"]["job"] is not None
    pid = response.json()["data"]["job"]["_id"]

    r2 = WorkerJobResult(job_id=pid, job_type=JobType.pow, pow_result="166189")
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=r2).model_dump(by_alias=True),
        headers={"Authorization": f"Bearer {worker2_jwt}"},
    )
    assert response.status_code == 200
    resp = FinishJobResponse.model_validate(response.json()["data"])
    assert resp.rewarded_points == 0.5

    # Verify job 2 in database
    j2 = get_jobs_info(mock_connections.state.conn.postgres, [pid])[0]
    assert j2.job_type == JobType.pow
    assert j2.result.pow_result == "166189"
    assert j2.worker == "worker2"
    assert j2.finished_at is not None

    # Case 3: job expired
    worker4_jwt = jwt_token("worker4")
    set_reward_stats(mock_connections.state.conn.redis, "worker4")
    response = client.get(
        "/take_job",
        params={"job_type": int(JobType.reward)},
        headers={"Authorization": f"Bearer {worker4_jwt}"},
    )
    assert response.status_code == 200
    job_id = response.json()["data"]["job"]["_id"]
    rewards = get_valid_rewards(mock_connections.state.conn.redis, "worker4")
    assert len(rewards.jobs) == 1

    # Manually expire the job in postgres by setting expired_at to a past time
    with closing(mock_connections.state.conn.postgres.cursor()) as cur:
        cur.execute(
            """
            UPDATE job_queue 
            SET lease_expired_at = EXTRACT(EPOCH FROM NOW())::BIGINT - 3600
            WHERE id = %s
            """,
            (job_id,),
        )
        mock_connections.state.conn.postgres.commit()

    r3 = WorkerJobResult(
        job_id=job_id, job_type=JobType.reward, reward_result=RewardResult()
    )
    response = client.post(
        "/finish_job",
        json=FinishJobRequest(job_result=r3).model_dump(by_alias=True),
        headers={"Authorization": f"Bearer {worker4_jwt}"},
    )
    assert response.status_code == 404
    assert response.json()["message"] == "lease not exists"

    rewards = get_valid_rewards(mock_connections.state.conn.redis, "worker4")
    assert len(rewards.jobs) == 0


@mongomock.patch((MOCK_MONGO_URL))
@mock_patch("requests.post")
def test_job_status(mock_requests, mock_connections):
    mock_requests.return_value = MockedHttpResponse(200)
    client = TestClient(mock_connections)
    classifier_id = client.app.state.classifier_id

    # Publish jobs
    bids = _publish_jobs_simple(client, JobType.batch_classify, 3, classifier_id)
    pids = _publish_jobs_simple(client, JobType.pow, 2)
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
        headers={"Authorization": f"Bearer {worker2_jwt}"},
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
            assert status["finishedAt"] != 0
        else:
            assert status["finishedAt"] == 0


@mongomock.patch((MOCK_MONGO_URL))
def test_register_classifier(mock_connections):
    client = TestClient(mock_connections)

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
    response = client.get("/classifier_info", params={"id": 1000})
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
def test_pow_validation(mock_requests, mock_connections):
    mock_requests.return_value = MockedHttpResponse(200)
    client = TestClient(mock_connections)
    # Publish a PoW job
    _publish_jobs_simple(client, JobType.pow, 1)
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


@mongomock.patch((MOCK_MONGO_URL))
def test_query_reward_jobs(mock_connections):
    client = TestClient(mock_connections)

    # Publish reward jobs
    job_ids = _publish_jobs(
        client,
        JobType.reward,
        [
            _build_reward_ctx(),
            _build_reward_ctx(Token(chain="arb", address="0x1234", protocol="ERC20")),
        ],
    )

    set_reward_stats(mock_connections.state.conn.redis, "worker1")

    # take reward jobs
    initial_time = datetime.datetime.now()
    worker1_jwt = jwt_token("worker1")
    response1 = client.get(
        "/take_job",
        params={"job_type": int(JobType.reward)},
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response1.status_code == 200

    second_time = initial_time + datetime.timedelta(0, 2400)  # 45 minutes later
    with freeze_time(second_time):
        response2 = client.get(
            "/take_job",
            params={"job_type": int(JobType.reward)},
            headers={"Authorization": f"Bearer {worker1_jwt}"},
        )
        assert response2.status_code == 200

    # query reward jobs
    response = client.get(
        "/reward_jobs",
        headers={"Authorization": f"Bearer {worker1_jwt}"},
    )
    assert response.status_code == 200
    records = RewardJobRecords.model_validate(response.json()["data"])
    assert len(records.jobs) == 2
    for job in records.jobs:
        assert job.job_id in job_ids
        assert job.reward_ctx is not None
