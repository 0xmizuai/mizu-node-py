import datetime
import os
from unittest import mock
from httpx import ASGITransport, AsyncClient
import testing.postgresql
import pytest
from fastapi import status
from unittest.mock import patch as mock_patch

from mizu_node.db.job_queue import (
    add_jobs,
    delete_one_job,
    get_assigned_reward_jobs,
    get_job_info,
)
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import (
    BatchClassifyContext,
    ClassifyResult,
    DataJobContext,
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
from tests.redis_mock import AsyncRedisMock

from tests.utils import (
    initiate_job_db,
    initiate_query_db,
    block_worker,
    clear_cooldown,
    set_reward_stats,
)
from freezegun import freeze_time

from unittest import mock
import pytest

from sqlalchemy.ext.asyncio import AsyncSession

API_SECRET_KEY = "some-secret"


class MockedConnections(Connections):
    def __init__(
        self,
        job_db_url: str | None = None,
        query_db_url: str | None = None,
    ):
        super().__init__(job_db_url, query_db_url, AsyncRedisMock())

    async def close(self):
        await self.job_db_engine.dispose()
        await self.query_db_engine.dispose()


class MockedHttpResponse:
    def __init__(self, status_code, json_data={}):
        self.status_code = status_code
        self.json_data = json_data

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if self.status_code != 200:
            raise Exception()


@pytest.fixture
def pg_db_url():
    with testing.postgresql.Postgresql() as postgresql:
        # Convert the dsn dictionary to a SQLAlchemy URL string
        dsn = postgresql.dsn()
        url = f"postgresql+asyncpg://{dsn['user']}@{dsn['host']}:{dsn['port']}/{dsn['database']}"
        yield url


@pytest.fixture()
def setenvvar(monkeypatch, pg_db_url):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "JOB_DB_URL": pg_db_url,
            "QUERY_DB_URL": pg_db_url,
            "REDIS_URL": "redis://localhost:6379",
            "API_SECRET_KEY": API_SECRET_KEY,
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


async def _publish_jobs(
    session: AsyncSession,
    job_type: JobType,
    payloads: list[RewardContext] | list[PowContext] | list[BatchClassifyContext],
):
    if job_type == JobType.pow:
        return await add_jobs(
            session,
            JobType.pow,
            [DataJobContext(pow_ctx=ctx) for ctx in payloads],
        )
    elif job_type == JobType.batch_classify:
        return await add_jobs(
            session,
            JobType.batch_classify,
            [DataJobContext(batch_classify_ctx=ctx) for ctx in payloads],
            1,
        )
    elif job_type == JobType.reward:
        return await add_jobs(
            session,
            JobType.reward,
            [DataJobContext(reward_ctx=ctx) for ctx in payloads],
        )
    else:
        raise ValueError(f"Invalid job type: {job_type}")


async def _publish_jobs_simple(conn: Connections, job_type, num_jobs=3):
    async with conn.get_job_db_session() as session:
        payloads = [_new_data_job_payload(job_type) for i in range(num_jobs)]
        return await _publish_jobs(session, job_type, payloads)


@pytest.fixture(scope="function")
async def mock_connections(monkeypatch, setenvvar, pg_db_url):
    monkeypatch.setattr("mizu_node.types.connections.Connections", MockedConnections)
    from mizu_node.node_server import app

    conn = MockedConnections(pg_db_url, pg_db_url)
    app.state.conn = conn

    async with conn.get_job_db_session() as session:
        await initiate_job_db(session)

    async with conn.get_query_db_session() as session:
        await initiate_query_db(session)
    return app


@pytest.mark.asyncio
async def test_publish_jobs_simple(mock_connections):
    app = await mock_connections

    job_ids1 = await _publish_jobs_simple(app.state.conn, JobType.reward, 3)
    assert len(job_ids1) == 3

    # Test publishing pow jobs
    job_ids2 = await _publish_jobs_simple(app.state.conn, JobType.pow, 3)
    assert len(job_ids2) == 3

    # Test publishing batch classify jobs
    job_ids3 = await _publish_jobs_simple(app.state.conn, JobType.batch_classify, 3)
    assert len(job_ids3) == 3
    await app.state.conn.close()


@pytest.mark.asyncio
async def test_take_job_ok(mock_connections):
    app = await mock_connections

    # Publish jobs first
    await _publish_jobs_simple(app.state.conn, JobType.pow, 3)
    await _publish_jobs_simple(app.state.conn, JobType.reward, 3)
    await _publish_jobs_simple(app.state.conn, JobType.batch_classify, 3)
    await set_reward_stats(app.state.conn.redis, "test_worker1")

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    ) as client:
        # Take reward job
        response1 = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.reward), "user": "test_worker1"},
        )
        assert response1.status_code == 200
        job1 = response1.json()["data"]["job"]
        assert job1["jobType"] == JobType.reward

        # Take pow job
        response2 = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.pow), "user": "test_worker2"},
        )
        assert response2.status_code == 200
        job2 = response2.json()["data"]["job"]
        assert job2["jobType"] == JobType.pow

        # Take batch classify job
        response3 = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.batch_classify), "user": "test_worker3"},
        )
        assert response3.status_code == 200
        job3 = response3.json()["data"]["job"]
        assert job3["jobType"] == JobType.batch_classify

    await app.state.conn.close()


@pytest.mark.asyncio
async def test_take_job_error(mock_connections):
    app = await mock_connections
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    ) as client:
        # No jobs published yet
        for _ in range(10):
            response = await client.get(
                "/take_job_v2",
                params={"job_type": int(JobType.pow), "user": "test_worker1"},
            )
            assert response.status_code == 200
            assert response.json()["data"].get("job", None) is None

        response = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.pow), "user": "test_worker1"},
        )
        assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS

        await _publish_jobs_simple(app.state.conn, JobType.pow, 3)
        response = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.batch_classify), "user": "test_worker2"},
        )
        assert response.status_code == 200
        assert response.json()["data"].get("job", None) is None

        # Blocked worker
        await block_worker(app.state.conn.redis, "test_worker3")
        response = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.batch_classify), "user": "test_worker3"},
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert response.json()["message"] == "worker is blocked"

        # user not active for reward job
        await _publish_jobs_simple(app.state.conn, JobType.reward, 3)
        response = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.reward), "user": "test_worker4"},
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert response.json()["message"] == "not active user"

        # cooling down
        response = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.reward), "user": "test_worker4"},
        )
        assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
        assert response.json()["message"].startswith("please retry after")

        # take reward job
        await clear_cooldown(app.state.conn.redis, "test_worker4", JobType.reward)
        await set_reward_stats(app.state.conn.redis, "test_worker4")
        response = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.reward), "user": "test_worker4"},
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["data"]["job"] is not None

        # should not be able to take another reward job
        await clear_cooldown(app.state.conn.redis, "test_worker4", JobType.reward)
        response = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.reward), "user": "test_worker4"},
        )
        assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
        assert response.json()["message"].startswith("please retry after")

    await app.state.conn.close()


@mock_patch("requests.post")
@pytest.mark.asyncio
async def test_finish_job(mock_requests, mock_connections):
    app = await mock_connections
    mock_requests.return_value = MockedHttpResponse(200)

    # Publish jobs
    await _publish_jobs_simple(app.state.conn, JobType.reward, 3)
    await _publish_jobs_simple(app.state.conn, JobType.batch_classify, 3)
    await _publish_jobs_simple(app.state.conn, JobType.pow, 3)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    ) as client:
        response1 = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.pow), "user": "test_worker1"},
        )
        assert response1.status_code == 200
        assert response1.json()["data"]["job"] is not None
        pid = response1.json()["data"]["job"]["_id"]

        # delete pow job 1 from database
        async with app.state.conn.get_job_db_session() as session:
            await delete_one_job(session, pid)
        # Try to finish job not in recorded - should fail
        result = WorkerJobResult(
            job_id=pid,  # job id not exists
            job_type=JobType.pow,
            pow_result="1234",
        )
        response2 = await client.post(
            "/finish_job_v2",
            json=FinishJobRequest(job_result=result, user="test_worker1").model_dump(),
        )
        assert response2.status_code == status.HTTP_404_NOT_FOUND
        assert response2.json()["message"] == "lease not exists"

        response2 = await client.post(
            "/finish_job_v2",
            json=FinishJobRequest(job_result=result, user="test_worker1").model_dump(),
        )
        assert response2.status_code == status.HTTP_404_NOT_FOUND

        response3 = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.batch_classify), "user": "test_worker3"},
        )
        assert response3.status_code == 200
        assert response3.json()["data"]["job"] is not None
        bid = response3.json()["data"]["job"]["_id"]

        # Case 1.1: finishing batch classify job with classify result
        response = await client.post(
            "/finish_job_v2",
            json={
                "jobResult": {
                    "jobId": bid,
                    "jobType": JobType.batch_classify,
                    "classifyResult": ["t1"],
                },
                "user": "test_worker3",
            },
        )
        assert response.status_code == 422

        # Case 1.2: wrong job result
        response = await client.post(
            "/finish_job_v2",
            json={
                "job_result": {
                    "job_id": bid,
                    "job_type": JobType.pow,
                    "classify_result": ["t1"],
                },
                "user": "test_worker3",
            },
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
        response = await client.post(
            "/finish_job_v2",
            json=FinishJobRequest(job_result=r13, user="test_worker3").model_dump(
                by_alias=True
            ),
        )
        assert response.status_code == 200
        assert response.json()["message"] == "ok"
        resp = FinishJobV2Response.model_validate(response.json()["data"])
        assert resp.settle_reward.amount == "0.5"

        # Verify job 1 in database
        async with app.state.conn.get_job_db_session() as session:
            j1 = await get_job_info(session, bid)
        assert j1["job_type"] == JobType.batch_classify
        # the empty labels are filtered out
        assert len(j1["result"]["batchClassifyResult"]) == 2
        assert j1["worker"] == "test_worker3"
        assert j1["finished_at"] is not None

        # Case 1.4: finishing finished jobs
        r14 = WorkerJobResult(
            job_id=bid,
            job_type=JobType.batch_classify,
            batch_classify_result=[
                ClassifyResult(uri="", text=""),
            ],
        )
        response = await client.post(
            "/finish_job_v2",
            json=FinishJobRequest(job_result=r14, user="test_worker3").model_dump(
                by_alias=True
            ),
        )
        assert response.status_code == 404

        # Case 2: job 2 finished by worker2
        response = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.pow), "user": "test_worker2"},
        )
        assert response.status_code == 200
        assert response.json()["data"]["job"] is not None
        pid = response.json()["data"]["job"]["_id"]

        r2 = WorkerJobResult(job_id=pid, job_type=JobType.pow, pow_result="166189")
        response = await client.post(
            "/finish_job_v2",
            json=FinishJobRequest(job_result=r2, user="test_worker2").model_dump(
                by_alias=True
            ),
        )
        assert response.status_code == 200
        resp = FinishJobV2Response.model_validate(response.json()["data"])
        assert resp.settle_reward.amount == "0.5"

        # Verify job 2 in database
        async with app.state.conn.get_job_db_session() as session:
            j2 = await get_job_info(session, pid)
        assert j2["job_type"] == JobType.pow
        assert j2["result"]["powResult"] == "166189"
        assert j2["worker"] == "test_worker2"
        assert j2["finished_at"] is not None

        # Case 3: job expired
        await set_reward_stats(app.state.conn.redis, "test_worker4")
        response = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.reward), "user": "test_worker4"},
        )
        assert response.status_code == 200
        job_id = response.json()["data"]["job"]["_id"]
        async with app.state.conn.get_job_db_session() as session:
            rewards = await get_assigned_reward_jobs(session, "test_worker4")
        assert len(rewards) == 1

        # Manually expire the job in postgres by setting expired_at to a past time
        async with app.state.conn.get_job_db_session() as session:
            conn = await session.connection()
            from sqlalchemy import text

            await conn.execute(
                text(
                    """
                UPDATE job_queue
                SET lease_expired_at = EXTRACT(EPOCH FROM NOW())::BIGINT - 3600
                WHERE id = :job_id
                """
                ),
                {"job_id": job_id},
            )

        r3 = WorkerJobResult(
            job_id=job_id, job_type=JobType.reward, reward_result=RewardResult()
        )
        response = await client.post(
            "/finish_job_v2",
            json=FinishJobRequest(job_result=r3, user="test_worker4").model_dump(
                by_alias=True
            ),
        )
        assert response.status_code == 404
        assert response.json()["message"] == "lease not exists"

        async with app.state.conn.get_job_db_session() as session:
            rewards = await get_assigned_reward_jobs(session, "test_worker4")
        assert len(rewards) == 0
    await app.state.conn.close()


@mock_patch("requests.post")
@pytest.mark.asyncio
async def test_pow_validation(mock_requests, mock_connections):
    mock_requests.return_value = MockedHttpResponse(200)
    app = await mock_connections
    await _publish_jobs_simple(app.state.conn, JobType.pow, 1)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    ) as client:
        # Take the job
        response = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.pow), "user": "test_worker1"},
        )
        assert response.status_code == 200
        job_id = response.json()["data"]["job"]["_id"]

        # Case 1: invalid pow result
        result = WorkerJobResult(
            job_id=job_id,
            job_type=JobType.pow,
            pow_result="invalid_nonce",
        )
        response = await client.post(
            "/finish_job_v2",
            json=FinishJobRequest(job_result=result, user="test_worker1").model_dump(
                by_alias=True
            ),
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
        response = await client.post(
            "/finish_job_v2",
            json=FinishJobRequest(job_result=result, user="test_worker1").model_dump(
                by_alias=True
            ),
        )
        assert response.status_code == 200
    await app.state.conn.close()


@pytest.mark.asyncio
async def test_query_reward_jobs(mock_connections):
    app = await mock_connections
    # Publish reward jobs
    async with app.state.conn.get_job_db_session() as session:
        job_ids = await _publish_jobs(
            session,
            JobType.reward,
            [
                _build_reward_ctx(),
                _build_reward_ctx(
                    Token(chain="arb", address="0x1234", protocol="ERC20")
                ),
            ],
        )
    await set_reward_stats(app.state.conn.redis, "test_worker1")

    # take reward jobs
    initial_time = datetime.datetime.now()

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
    ) as client:
        response1 = await client.get(
            "/take_job_v2",
            params={"job_type": int(JobType.reward), "user": "test_worker1"},
            headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
        )
        assert response1.status_code == 200

        second_time = initial_time + datetime.timedelta(0, 2400)  # 45 minutes later
        with freeze_time(second_time):
            response2 = await client.get(
                "/take_job_v2",
                params={"job_type": int(JobType.reward), "user": "test_worker1"},
                headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
            )
            assert response2.status_code == 200

        # query reward jobs
        response = await client.get(
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
    await app.state.conn.close()
