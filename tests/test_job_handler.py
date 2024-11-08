from uuid import uuid4
from fastapi import HTTPException
import pytest
from redis import Redis
from fastapi import status
from unittest.mock import patch as mock_patch

import mizu_node.job_handler as job_handler
from mizu_node.security import block_worker
from mizu_node.types.job import (
    ClassifyContext,
    DataJobPayload,
    JobType,
    PowContext,
    PublishJobRequest,
    WorkerJobResult,
)
from tests.job_queue_mock import JobQueueMock
from tests.mongo_mock import MongoMock
from tests.redis_mock import RedisMock


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


def _publish_jobs(mdb: MongoMock, job_type: JobType, num_jobs=3):
    payloads = [_new_data_job_payload(job_type, str(i + 1)) for i in range(num_jobs)]
    return list(
        job_handler.handle_publish_jobs(
            mdb, "worker1", PublishJobRequest(data=payloads)
        )
    )


@mock_patch("mizu_node.job_handler.job_queue")
def test_publish_jobs(mock_job_queue):
    job_queues = {
        JobType.classify: JobQueueMock("classify"),
        JobType.pow: JobQueueMock("pow"),
    }
    mock_job_queue.side_effect = lambda job_type: job_queues[job_type]
    mdb = MongoMock()

    job_ids1 = _publish_jobs(mdb, JobType.classify, 3)
    assert len(job_ids1) == 3
    assert job_queues[JobType.classify].queue_len() == 3

    job_ids2 = _publish_jobs(mdb, JobType.pow, 3)
    assert len(job_ids2) == 3
    assert job_queues[JobType.pow].queue_len() == 3


@mock_patch("mizu_node.job_handler.job_queue")
def test_take_job_ok(mock_job_queue):
    job_queues = {
        JobType.classify: JobQueueMock("classify"),
        JobType.pow: JobQueueMock("pow"),
    }
    mock_job_queue.side_effect = lambda job_type: job_queues[job_type]
    rclient = RedisMock()
    mdb = MongoMock()
    pids = _publish_jobs(mdb, JobType.pow, 3)
    cids = _publish_jobs(mdb, JobType.classify, 3)

    # take classify job 1
    job1 = job_handler.handle_take_job(rclient, "worker1", JobType.classify)
    assert job1.job_id == cids[0]
    assert job1.job_type == JobType.classify
    assert job_queues[JobType.classify].queue_len() == 3

    # take pow job 1
    job2 = job_handler.handle_take_job(rclient, "worker2", JobType.pow)
    assert job2.job_id == pids[0]
    assert job2.job_type == JobType.pow
    assert job_queues[JobType.pow].queue_len() == 3


@mock_patch("mizu_node.job_handler.job_queue")
def test_take_job_error(mock_job_queue):
    job_queues = {
        JobType.classify: JobQueueMock("classify"),
        JobType.pow: JobQueueMock("pow"),
    }
    mock_job_queue.side_effect = lambda job_type: job_queues[job_type]
    rclient = RedisMock()
    mdb = MongoMock()
    # No jobs published yet
    job = job_handler.handle_take_job(rclient, "worker1", JobType.classify)
    assert job is None

    _publish_jobs(mdb, JobType.pow, 3)
    job = job_handler.handle_take_job(rclient, "worker1", JobType.classify)
    assert job is None

    block_worker(rclient, "worker1")
    with pytest.raises(HTTPException) as e:
        job_handler.handle_take_job(rclient, "worker1", JobType.classify)
    assert e.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert e.value.detail == "worker is blocked"


@mock_patch("mizu_node.job_handler.job_queue")
def test_finish_job_ok(mock_job_queue):
    job_queues = {
        JobType.classify: JobQueueMock("classify"),
        JobType.pow: JobQueueMock("pow"),
    }
    mock_job_queue.side_effect = lambda job_type: job_queues[job_type]
    rclient = RedisMock()
    mdb = MongoMock()
    cids = _publish_jobs(mdb, JobType.classify, 3)
    pids = _publish_jobs(mdb, JobType.pow, 3)

    job_handler.handle_take_job(rclient, "worker1", JobType.classify)
    job_handler.handle_take_job(rclient, "worker2", JobType.pow)

    # Case 1: job 1 finished by worker1
    r1 = WorkerJobResult(
        job_id=cids[0],
        job_type=JobType.classify,
        classify_result=["t1"],
    )
    job_handler.handle_finish_job(rclient, mdb, "worker1", r1)
    j1 = mdb.find_one({"_id": cids[0]})
    assert j1["jobType"] == JobType.classify
    assert j1["classifyResult"] == ["t1"]
    assert j1["worker"] == "worker1"
    assert j1["finishedAt"] is not None

    # Case 2: job 2 finished by worker2
    r2 = WorkerJobResult(job_id=pids[0], job_type=JobType.pow, pow_result="0x")
    job_handler.handle_finish_job(rclient, mdb, "worker2", r2)
    j2 = mdb.find_one({"_id": pids[0]})
    assert j2["jobType"] == JobType.pow
    assert j2["powResult"] == "0x"
    assert j2["worker"] == "worker2"
    assert j2["finishedAt"] is not None


@mock_patch("mizu_node.job_handler.job_queue")
def test_finish_job_error(mock_job_queue):
    job_queues = {
        JobType.classify: JobQueueMock("classify"),
        JobType.pow: JobQueueMock("pow"),
    }
    mock_job_queue.side_effect = lambda job_type: job_queues[job_type]
    rclient = RedisMock()
    mdb = MongoMock()

    cids = _publish_jobs(mdb, JobType.classify, 3)
    job = job_handler.handle_take_job(rclient, "worker1", JobType.classify)

    r2 = WorkerJobResult(
        job_id=job.job_id,
        job_type=JobType.classify,
        classify_result=["t1"],
    )
    job_handler.handle_finish_job(rclient, mdb, "worker1", r2)
    with pytest.raises(HTTPException) as e:
        job_handler.handle_finish_job(rclient, mdb, "worker1", r2)
    assert e.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert e.value.detail == "job already finished"
