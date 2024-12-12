from contextlib import closing, contextmanager
import datetime
import os
from unittest import mock
import testing.postgresql
from fastapi.testclient import TestClient
import psycopg2
import pytest
from fastapi import status
from unittest.mock import patch as mock_patch

from mizu_node.db.api_key import create_api_key
from mizu_node.db.job_queue import add_jobs, delete_one_job, get_assigned_reward_jobs
from mizu_node.db.job_queue import get_jobs_info
from mizu_node.types.data_job import (
    BatchClassifyContext,
    ClassifyResult,
    DataJobContext,
    JobStatus,
    JobType,
    PowContext,
    RewardContext,
    RewardResult,
    Token,
    WorkerJobResult,
)

from mizu_node.types.node_service import (
    FinishJobRequest,
    FinishJobV2Response,
    QueryRewardJobsResponse,
)
from tests.redis_mock import RedisMock

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from tests.utils import initiate_pg_db
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
API_SECRET_KEY = "some-secret"


# Mock the Connections class
class MockConnections:
    def __init__(self, postgres=None, redis=None):
        self.postgres = postgres
        self.redis = redis

    @contextmanager
    def get_pg_connection(self):
        yield self.postgres


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


@pytest.fixture()
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "POSTGRES_URL": "postgresql://postgres:postgres@localhost:5432/postgres",
            "REDIS_URL": "redis://localhost:6379",
            "JWT_VERIFY_KEY": public_key_str,
            "API_SECRET_KEY": API_SECRET_KEY,
            "BACKEND_SERVICE_URL": "http://localhost:3000",
            "WORKFLOW_SERVER_URL": "http://localhost:3000",
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


def _build_batch_classify_ctx():
    return BatchClassifyContext(
        data_url="https://example.com/data",
        batch_size=1000,
        bytesize=5000000,
        decompressed_byte_size=20000000,
        checksum_md5="0x",
        classifier_id=1,
    )


def _build_reward_ctx(token: Token | None = None):
    return RewardContext(token=token, amount=50)


def _new_data_job_payload(
    job_type: str,
) -> RewardContext | PowContext | BatchClassifyContext:
    if job_type == JobType.reward:
        return _build_reward_ctx()
    elif job_type == JobType.pow:
        return _build_pow_ctx()
    elif job_type == JobType.batch_classify:
        return _build_batch_classify_ctx()
    else:
        raise ValueError(f"Invalid job type: {job_type}")


def _publish_jobs(
    mock_connections: MockConnections,
    job_type: JobType,
    payloads: list[RewardContext] | list[PowContext] | list[BatchClassifyContext],
):
    if job_type == JobType.pow:
        return add_jobs(
            mock_connections.postgres,
            JobType.pow,
            "test_user1",
            [DataJobContext(pow_ctx=ctx) for ctx in payloads],
        )
    elif job_type == JobType.batch_classify:
        return add_jobs(
            mock_connections.postgres,
            JobType.batch_classify,
            "test_user1",
            [DataJobContext(reward_ctx=ctx) for ctx in payloads],
        )
    elif job_type == JobType.reward:
        return add_jobs(
            mock_connections.postgres,
            JobType.reward,
            "test_user1",
            [DataJobContext(reward_ctx=ctx) for ctx in payloads],
        )
    else:
        raise ValueError(f"Invalid job type: {job_type}")


def _publish_jobs_simple(
    mock_connections: MockConnections,
    job_type: JobType,
    num_jobs=3,
):
    payloads = [_new_data_job_payload(job_type) for i in range(num_jobs)]
    return _publish_jobs(mock_connections, job_type, payloads)


@pytest.fixture(scope="function")
def mock_connections(monkeypatch, pg_conn, setenvvar):
    monkeypatch.setattr("mizu_node.types.connections.Connections", MockConnections)
    mock_conn = MockConnections(postgres=pg_conn, redis=RedisMock())

    from mizu_node.node_server import app

    app.state.conn = mock_conn

    with mock_conn.get_pg_connection() as pg_conn:
        initiate_pg_db(pg_conn)
        create_api_key(pg_conn, "test_user1", TEST_API_KEY1)
        create_api_key(pg_conn, "test_user2", TEST_API_KEY2)
    return app


def test_publish_jobs_simple(mock_connections):
    client = TestClient(mock_connections)

    # Test publishing classify jobs
    job_ids1 = _publish_jobs_simple(mock_connections.state.conn, JobType.reward, 3)
    assert len(job_ids1) == 3

    # Test publishing pow jobs
    job_ids2 = _publish_jobs_simple(mock_connections.state.conn, JobType.pow, 3)
    assert len(job_ids2) == 3

    # Test publishing batch classify jobs
    job_ids3 = _publish_jobs_simple(
        mock_connections.state.conn, JobType.batch_classify, 3
    )
    assert len(job_ids3) == 3


def test_take_job_ok(mock_connections):
    client = TestClient(mock_connections)

    # Publish jobs first
    _publish_jobs_simple(mock_connections.state.conn, JobType.pow, 3)
    _publish_jobs_simple(mock_connections.state.conn, JobType.reward, 3)
    _publish_jobs_simple(mock_connections.state.conn, JobType.batch_classify, 3)

    # Take reward job 1
    set_reward_stats(mock_connections.state.conn.redis, "test_worker1")
    response1 = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.reward), "user": "test_worker1"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response1.status_code == 200
    job1 = response1.json()["data"]["job"]
    assert job1["jobType"] == JobType.reward

    # Take pow job 1
    response2 = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.pow), "user": "test_worker2"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response2.status_code == 200
    job2 = response2.json()["data"]["job"]
    assert job2["jobType"] == JobType.pow

    # Take batch classify job
    response3 = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.batch_classify), "user": "test_worker3"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response3.status_code == 200
    job3 = response3.json()["data"]["job"]
    assert job3["jobType"] == JobType.batch_classify


def test_take_job_error(mock_connections):
    client = TestClient(mock_connections)

    # No jobs published yet
    for _ in range(10):
        response = client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.pow), "user": "test_worker1"},
            headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
        )
        assert response.status_code == 200
        assert response.json()["data"].get("job", None) is None

    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.pow), "user": "test_worker1"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS

    # Wrong job type (pow jobs published, trying to take batch classify job)
    _publish_jobs_simple(mock_connections.state.conn, JobType.pow, 3)
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.batch_classify), "user": "test_worker2"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 200
    assert response.json()["data"].get("job", None) is None

    # Blocked worker
    block_worker(mock_connections.state.conn.redis, "test_worker3")
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.batch_classify), "user": "test_worker3"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert response.json()["message"] == "worker is blocked"

    # user not active for reward job
    _publish_jobs_simple(mock_connections.state.conn, JobType.reward, 3)
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.reward), "user": "test_worker4"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert response.json()["message"] == "not active user"

    # cooling down
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.reward), "user": "test_worker4"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
    assert response.json()["message"].startswith("please retry after")

    # take reward job
    clear_cooldown(mock_connections.state.conn.redis, "test_worker4", JobType.reward)
    set_reward_stats(mock_connections.state.conn.redis, "test_worker4")
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.reward), "user": "test_worker4"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["data"]["job"] is not None

    # should not be able to take another reward job
    clear_cooldown(mock_connections.state.conn.redis, "test_worker4", JobType.reward)
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.reward), "user": "test_worker4"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
    assert response.json()["message"].startswith("please retry after")


@mock_patch("requests.post")
def test_finish_job(mock_requests, mock_connections):
    client = TestClient(mock_connections)

    mock_requests.return_value = MockedHttpResponse(200)

    # Publish jobs
    _publish_jobs_simple(mock_connections.state.conn, JobType.reward, 3)
    _publish_jobs_simple(mock_connections.state.conn, JobType.batch_classify, 3)
    _publish_jobs_simple(mock_connections.state.conn, JobType.pow, 3)

    response1 = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.pow), "user": "test_worker1"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
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
        "/finish_job_v2",
        json=FinishJobRequest(job_result=result, user="test_worker1").model_dump(),
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response2.status_code == status.HTTP_404_NOT_FOUND
    assert response2.json()["message"] == "lease not exists"

    response2 = client.post(
        "/finish_job_v2",
        json=FinishJobRequest(job_result=result, user="test_worker1").model_dump(),
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response2.status_code == status.HTTP_404_NOT_FOUND

    response3 = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.batch_classify), "user": "test_worker3"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response3.status_code == 200
    assert response3.json()["data"]["job"] is not None
    bid = response3.json()["data"]["job"]["_id"]

    # Case 1.1: finishing batch classify job with classify result
    response = client.post(
        "/finish_job_v2",
        json={
            "jobResult": {
                "jobId": bid,
                "jobType": JobType.batch_classify,
                "classifyResult": ["t1"],
            },
            "user": "test_worker3",
        },
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 422

    # Case 1.2: wrong job result
    response = client.post(
        "/finish_job_v2",
        json={
            "job_result": {
                "job_id": bid,
                "job_type": JobType.batch_classify,
                "classify_result": ["t1"],
            },
            "user": "test_worker3",
        },
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 422

    # Case 1.3: finishing batch classify job
    r13 = WorkerJobResult(
        job_id=bid,
        job_type=JobType.batch_classify,
        batch_classify_result=[
            ClassifyResult(uri="123", text="123"),
            ClassifyResult(uri="234", text="234"),
        ],
    )
    response = client.post(
        "/finish_job_v2",
        json=FinishJobRequest(job_result=r13, user="test_worker3").model_dump(
            by_alias=True
        ),
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 200
    assert response.json()["message"] == "ok"
    resp = FinishJobV2Response.model_validate(response.json()["data"])
    assert resp.settle_reward.amount == "0.5"

    # Verify job 1 in database
    j1 = get_jobs_info(mock_connections.state.conn.postgres, [bid])[0]
    assert j1.job_type == JobType.batch_classify
    # the empty labels are filtered out
    assert len(j1.result.batch_classify_result) == 2
    assert j1.worker == "test_worker3"
    assert j1.finished_at is not None

    # Case 1.4: finishing finished jobs
    r14 = WorkerJobResult(
        job_id=bid,
        job_type=JobType.batch_classify,
        batch_classify_result=[
            ClassifyResult(uri="", text=""),
        ],
    )
    response = client.post(
        "/finish_job_v2",
        json=FinishJobRequest(job_result=r14, user="test_worker3").model_dump(
            by_alias=True
        ),
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 404

    # Case 2: job 2 finished by worker2
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.pow), "user": "test_worker2"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 200
    assert response.json()["data"]["job"] is not None
    pid = response.json()["data"]["job"]["_id"]

    r2 = WorkerJobResult(job_id=pid, job_type=JobType.pow, pow_result="166189")
    response = client.post(
        "/finish_job_v2",
        json=FinishJobRequest(job_result=r2, user="test_worker2").model_dump(
            by_alias=True
        ),
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 200
    resp = FinishJobV2Response.model_validate(response.json()["data"])
    assert resp.settle_reward.amount == "0.5"

    # Verify job 2 in database
    j2 = get_jobs_info(mock_connections.state.conn.postgres, [pid])[0]
    assert j2.job_type == JobType.pow
    assert j2.result.pow_result == "166189"
    assert j2.worker == "test_worker2"
    assert j2.finished_at is not None

    # Case 3: job expired
    set_reward_stats(mock_connections.state.conn.redis, "test_worker4")
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.reward), "user": "test_worker4"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 200
    job_id = response.json()["data"]["job"]["_id"]
    rewards = get_assigned_reward_jobs(
        mock_connections.state.conn.postgres, "test_worker4"
    )
    assert len(rewards) == 1

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
        "/finish_job_v2",
        json=FinishJobRequest(job_result=r3, user="test_worker4").model_dump(
            by_alias=True
        ),
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 404
    assert response.json()["message"] == "lease not exists"

    rewards = get_assigned_reward_jobs(
        mock_connections.state.conn.postgres, "test_worker4"
    )
    assert len(rewards) == 0


@mock_patch("requests.post")
def test_job_status(mock_requests, mock_connections):
    mock_requests.return_value = MockedHttpResponse(200)
    client = TestClient(mock_connections)

    # Publish jobs
    bids = _publish_jobs_simple(mock_connections.state.conn, JobType.batch_classify, 3)
    pids = _publish_jobs_simple(mock_connections.state.conn, JobType.pow, 2)
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
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.batch_classify), "user": "test_worker1"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 200
    classify_job = response.json()["data"]["job"]

    classify_result = ClassifyResult(uri="", text="")
    result1 = WorkerJobResult(
        job_id=classify_job["_id"],
        job_type=JobType.batch_classify,
        batch_classify_result=[classify_result],
    )
    response = client.post(
        "/finish_job_v2",
        json=FinishJobRequest(job_result=result1, user="test_worker1").model_dump(
            by_alias=True
        ),
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 200

    # Take and finish a pow job
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.pow), "user": "test_worker2"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 200
    pow_job = response.json()["data"]["job"]

    result2 = WorkerJobResult(
        job_id=pow_job["_id"], job_type=JobType.pow, pow_result="166189"
    )
    response = client.post(
        "/finish_job_v2",
        json=FinishJobRequest(job_result=result2, user="test_worker2").model_dump(
            by_alias=True
        ),
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
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


@mock_patch("requests.post")
def test_pow_validation(mock_requests, mock_connections):
    mock_requests.return_value = MockedHttpResponse(200)
    client = TestClient(mock_connections)
    # Publish a PoW job
    _publish_jobs_simple(mock_connections.state.conn, JobType.pow, 1)

    # Take the job
    response = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.pow), "user": "test_worker1"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
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
        "/finish_job_v2",
        json=FinishJobRequest(job_result=result, user="test_worker1").model_dump(
            by_alias=True
        ),
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
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
        "/finish_job_v2",
        json=FinishJobRequest(job_result=result, user="test_worker1").model_dump(
            by_alias=True
        ),
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 200


def test_query_reward_jobs(mock_connections):
    client = TestClient(mock_connections)

    # Publish reward jobs
    job_ids = _publish_jobs(
        mock_connections.state.conn,
        JobType.reward,
        [
            _build_reward_ctx(),
            _build_reward_ctx(Token(chain="arb", address="0x1234", protocol="ERC20")),
        ],
    )

    set_reward_stats(mock_connections.state.conn.redis, "test_worker1")

    # take reward jobs
    initial_time = datetime.datetime.now()
    response1 = client.get(
        "/take_job_v2",
        params={"job_type": int(JobType.reward), "user": "test_worker1"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response1.status_code == 200

    second_time = initial_time + datetime.timedelta(0, 2400)  # 45 minutes later
    with freeze_time(second_time):
        response2 = client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.reward), "user": "test_worker1"},
            headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
        )
        assert response2.status_code == 200

    # query reward jobs
    response = client.get(
        "/reward_jobs_v2",
        params={"user": "test_worker1"},
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    )
    assert response.status_code == 200
    records = QueryRewardJobsResponse.model_validate(response.json()["data"])
    assert len(records.jobs) == 2
    for job in records.jobs:
        assert job.job_id in job_ids
        assert job.reward_ctx is not None
