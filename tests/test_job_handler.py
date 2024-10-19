import pytest

from mizu_node.constants import (
    VERIFY_JOB_QUEUE_NAME,
)
from mizu_node.types import (
    DataJob,
    DataJobPayload,
    JobType,
    FinishedJob,
    PublishJobRequest,
    WorkerJob,
    WorkerJobResult,
)
import mizu_node.job_handler as job_handler
from mizu_node.utils import epoch
from tests.mongo_mock import MongoMock
from tests.redis_mock import RedisMock
from mizu_node.job_queue import job_queues


def _new_data_job_payload(key: str):
    return DataJobPayload(
        publisher="p",
        published_at=epoch(),
        input=key,
    )


def _add_new_jobs(rclient, job_type: JobType, num_jobs=3):
    jobs = [_new_data_job_payload(str(i + 1)) for i in range(num_jobs)]
    req = PublishJobRequest(job_type=job_type, jobs=jobs)
    return job_handler.handle_publish_jobs(rclient, req)


def test_publish_jobs():
    rclient = RedisMock()
    _add_new_jobs(rclient, JobType.classification, 3)
    job_queues[JobType.classification].queue_len(rclient) == 3
    job_queues[JobType.classification].processing_len(rclient) == 0

    _add_new_jobs(rclient, JobType.pow, 3)
    job_queues[JobType.pow].queue_len(rclient) == 3
    job_queues[JobType.pow].processing_len(rclient) == 0


def test_take_job_ok():
    rclient = RedisMock()
    pids = _add_new_jobs(rclient, JobType.pow, 3)
    cids = _add_new_jobs(rclient, JobType.classification, 3)

    # take classification job 1
    job1 = job_handler.handle_take_job(rclient, "worker1", [JobType.classification])
    assert job1.job_id == cids[0]
    assert job1.job_type == JobType.classification
    job_queues[JobType.classification].queue_len(rclient) == 3
    job_queues[JobType.classification].processing_len(rclient) == 1

    # take pow job 1
    job2 = job_handler.handle_take_job(rclient, "worker2", [JobType.pow])
    assert job2.job_id == pids[0]
    assert job2.job_type == JobType.pow
    job_queues[JobType.pow].queue_len(rclient) == 3
    job_queues[JobType.pow].processing_len(rclient) == 1

    # take classification job 2
    job3 = job_handler.handle_take_job(rclient, "worker3")
    assert job3.job_id == cids[1]
    assert job3.job_type == JobType.classification

    job_queues[JobType.classification].queue_len(rclient) == 3
    job_queues[JobType.classification].processing_len(rclient) == 2


def test_take_job_error():
    rclient = RedisMock()
    with pytest.raises(ValueError) as e:
        job_handler.handle_take_job(rclient, "worker1")
    assert e.match("no job available")

    _add_new_jobs(rclient, JobType.pow, 3)
    with pytest.raises(ValueError) as e:
        job_handler.handle_take_job(rclient, "worker1", [JobType.classification])
    assert e.match("no job available")

    job_handler._block_worker(rclient, "worker1")
    with pytest.raises(ValueError) as e:
        job_handler.handle_take_job(rclient, "worker1")
    assert e.match("worker is blocked")


def test_finish_job_ok():
    rclient = RedisMock()
    mdb = MongoMock()
    cids = _add_new_jobs(rclient, JobType.classification, 3)
    pids = _add_new_jobs(rclient, JobType.pow, 3)

    job_handler.handle_take_job(rclient, "worker1", [JobType.classification])
    job_handler.handle_take_job(rclient, "worker2", [JobType.pow])

    # Case 1: job 1 finished by worker1
    r1 = WorkerJobResult(
        job_id=cids[0], job_type=JobType.classification, worker="worker1", output=["t1"]
    )
    job_handler.handle_finish_job(rclient, mdb, r1)
    j1 = mdb.find_one({"_id": cids[0]})
    assert j1["output"] == ["t1"]
    assert j1["worker"] == "worker1"
    assert j1["finished_at"] is not None
    assert j1["job_type"] == JobType.classification

    # Case 2: job 2 finished by worker2
    r2 = WorkerJobResult(
        job_id=pids[0], job_type=JobType.pow, worker="worker2", output=["t2"]
    )
    job_handler.handle_finish_job(rclient, mdb, r2)
    j2 = mdb.find_one({"_id": pids[0]})
    assert j2["output"] == ["t2"]
    assert j2["worker"] == "worker2"
    assert j2["finished_at"] is not None
    assert j2["job_type"] == JobType.pow


def test_finish_job_verify(mocker):
    rclient = RedisMock()
    mdb = MongoMock()
    cids = _add_new_jobs(rclient, JobType.classification, 3)
    pids = _add_new_jobs(rclient, JobType.pow, 3)

    job_handler.handle_take_job(rclient, "worker1", [JobType.classification])
    job_handler.handle_take_job(rclient, "worker2", [JobType.pow])

    r1 = WorkerJobResult(
        job_id=cids[0], job_type=JobType.classification, worker="worker1", output=["t1"]
    )
    mocker.patch("mizu_node.job_handler._should_verify", return_value=False)
    job_handler.handle_finish_job(rclient, mdb, r1)
    assert not rclient.exists(VERIFY_JOB_QUEUE_NAME)

    r2 = WorkerJobResult(
        job_id=pids[0], job_type=JobType.pow, worker="worker2", output=["t2"]
    )
    mocker.patch("mizu_node.job_handler._should_verify", return_value=True)
    job_json = job_queues[JobType.pow].get_item_data(rclient, r2.job_id)
    data_job = DataJob.model_validate_json(job_json)
    job_handler.handle_finish_job(rclient, mdb, r2)
    worker_job = WorkerJob.model_validate_json(rclient.rpop(VERIFY_JOB_QUEUE_NAME))
    assert worker_job.job_id == data_job.job_id


def test_finish_job_error():
    rclient = RedisMock()
    mdb = MongoMock()
    _add_new_jobs(rclient, JobType.classification, 3)
    job = job_handler.handle_take_job(rclient, "worker1", [JobType.classification])

    r2 = WorkerJobResult(
        job_id=job.job_id,
        job_type=JobType.classification,
        worker="worker1",
        output=["t1"],
    )
    job_handler.handle_finish_job(rclient, mdb, r2)
    with pytest.raises(ValueError) as e2:
        job_handler.handle_finish_job(rclient, mdb, r2)
    assert e2.match("invalid job")


def test_verify_job_pass():
    rclient = RedisMock()
    mdb = MongoMock()

    jresult = FinishedJob(
        job_id="1",
        job_type=JobType.classification,
        input="1",
        publisher="p1",
        published_at=epoch() - 6000,
        worker="worker1",
        finished_at=epoch() - 3600,
        output=["t1", "t2"],
    )
    job_handler._save_finished_job(mdb, jresult)
    assert not job_handler._is_worker_blocked(rclient, "worker1")

    result = WorkerJobResult(
        job_id="1",
        job_type=JobType.classification,
        worker="worker1",
        output=["t1", "t2"],
    )
    job_handler.handle_verify_job_result(rclient, mdb, result)
    assert not job_handler._is_worker_blocked(rclient, "worker1")


def test_verify_job_blocked():
    rclient = RedisMock()
    mdb = MongoMock()

    jresult = FinishedJob(
        job_id="1",
        job_type=JobType.classification,
        input="1",
        publisher="p1",
        published_at=epoch() - 6000,
        worker="worker1",
        finished_at=epoch() - 3600,
        output=["t1", "t2"],
    )
    job_handler._save_finished_job(mdb, jresult)
    assert not job_handler._is_worker_blocked(rclient, "worker1")

    result = WorkerJobResult(
        job_id="1", job_type=JobType.classification, worker="worker1", output=["t2"]
    )
    job_handler.handle_verify_job_result(rclient, mdb, result)
    assert job_handler._is_worker_blocked(rclient, "worker1")


def test_verify_job_error():
    rclient = RedisMock()
    mdb = MongoMock()

    result = WorkerJobResult(
        job_id="1", job_type=JobType.classification, worker="worker1", output=["t2"]
    )
    with pytest.raises(ValueError) as e:
        job_handler.handle_verify_job_result(rclient, mdb, result)
    assert e.match("invalid job")
