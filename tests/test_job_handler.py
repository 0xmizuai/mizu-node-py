from uuid import uuid4
from fastapi import HTTPException
import pytest
from redis import Redis
from fastapi import status

import mizu_node.job_handler as job_handler
from mizu_node.security import block_worker
from mizu_node.types.common import JobType
from mizu_node.types.data_job import (
    ClassifyContext,
    DataJobPayload,
    PowContext,
    PublishJobRequest,
    WorkerJobResult,
)
from tests.mongo_mock import MongoMock
from tests.redis_mock import RedisMock
from mizu_node.job_handler import job_queues


pow_queue = job_queues[JobType.pow]
classify_queue = job_queues[JobType.classify]


def _build_classify_ctx(url: str):
    return ClassifyContext(
        r2_url=url,
        byte_size=1,
        checksum="0x",
    )


def _build_pow_ctx():
    return PowContext(difficulty=1, seed=str(uuid4()))


def _new_data_job_payload(job_type: str, r2_url: str) -> DataJobPayload:
    if job_type == JobType.classify:
        return DataJobPayload(
            job_type=JobType.classify, classify_ctx=_build_classify_ctx(r2_url)
        )
    else:
        return DataJobPayload(job_type=JobType.pow, pow_ctx=_build_pow_ctx())


def _publish_jobs(rclient: Redis, job_type: JobType, num_jobs=3):
    payloads = [_new_data_job_payload(job_type, str(i + 1)) for i in range(num_jobs)]
    return job_handler.handle_publish_jobs(
        rclient, "worker1", PublishJobRequest(data=payloads)
    )


def test_publish_jobs():
    rclient = RedisMock()
    job_ids1 = _publish_jobs(rclient, JobType.classify, 3)
    assert len(job_ids1) == 3
    classify_queue.queue_len(rclient) == 3
    classify_queue.processing_len(rclient) == 0

    job_ids2 = _publish_jobs(rclient, JobType.pow, 3)
    assert len(job_ids2) == 3
    pow_queue.queue_len(rclient) == 3
    pow_queue.processing_len(rclient) == 0


def test_take_job_ok():
    rclient = RedisMock()
    pids = _publish_jobs(rclient, JobType.pow, 3)
    cids = _publish_jobs(rclient, JobType.classify, 3)

    # take classify job 1
    job1 = job_handler.handle_take_job(rclient, "worker1", JobType.classify)
    assert job1.job_id == cids[0]
    assert job1.job_type == JobType.classify
    classify_queue.queue_len(rclient) == 3
    classify_queue.processing_len(rclient) == 1

    # take pow job 1
    job2 = job_handler.handle_take_job(rclient, "worker2", JobType.pow)
    assert job2.job_id == pids[0]
    assert job2.job_type == JobType.pow
    pow_queue.queue_len(rclient) == 3
    pow_queue.processing_len(rclient) == 1


def test_take_job_error():
    rclient = RedisMock()
    job = job_handler.handle_take_job(rclient, "worker1", JobType.classify)
    assert job is None

    _publish_jobs(rclient, JobType.pow, 3)
    job = job_handler.handle_take_job(rclient, "worker1", JobType.classify)
    assert job is None

    block_worker(rclient, "worker1")
    with pytest.raises(HTTPException) as e:
        job_handler.handle_take_job(rclient, "worker1", JobType.classify)
    assert e.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert e.value.detail == "worker is blocked"


def test_finish_job_ok():
    rclient = RedisMock()
    mdb = MongoMock()
    cids = _publish_jobs(rclient, JobType.classify, 3)
    pids = _publish_jobs(rclient, JobType.pow, 3)

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
    assert j1["job_type"] == JobType.classify
    assert j1["classify_result"] == ["t1"]
    assert j1["worker"] == "worker1"
    assert j1["finished_at"] is not None

    # Case 2: job 2 finished by worker2
    r2 = WorkerJobResult(job_id=pids[0], job_type=JobType.pow, pow_result="0x")
    job_handler.handle_finish_job(rclient, mdb, "worker2", r2)
    j2 = mdb.find_one({"_id": pids[0]})
    assert j2["job_type"] == JobType.pow
    assert j2["pow_result"] == "0x"
    assert j2["worker"] == "worker2"
    assert j2["finished_at"] is not None


def test_finish_job_error():
    rclient = RedisMock()
    mdb = MongoMock()
    _publish_jobs(rclient, JobType.classify, 3)
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
    assert e.value.detail == "job expired or not exists"
