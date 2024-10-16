from unittest.mock import patch
import pytest

from mizu_node.constants import (
    FINISH_JOB_CALLBACK_URL,
    REDIS_PENDING_JOBS_QUEUE,
    VERIFY_JOB_CALLBACK_URL,
)
from mizu_node.types import (
    JobType,
    PendingJob,
    AssignedJob,
    FinishedJob,
    WorkerJob,
    WorkerJobResult,
)
import mizu_node.job_handler as job_handler
from mizu_node.utils import epoch
from tests.mongo_mock import MongoMock
from tests.redis_mock import RedisMock


def _new_pending_job(key: str):
    return PendingJob(
        job_id=key,
        job_type=JobType.classification,
        publisher="p" + key,
        published_at=epoch(),
        input=key,
    )


def _add_new_jobs(rclient, num_jobs=3):
    jobs = [_new_pending_job(str(i + 1)) for i in range(num_jobs)]
    job_handler.handle_publish_jobs(rclient, jobs)


def test_new_jobs():
    rclient = RedisMock()
    _add_new_jobs(rclient, 3)
    assert job_handler.get_pending_jobs_num(rclient) == 3
    rclient.get(REDIS_PENDING_JOBS_QUEUE) == ["1", "2", "3"]


def test_take_job_ok():
    rclient = RedisMock()
    _add_new_jobs(rclient, 3)

    job1 = job_handler.handle_take_job(rclient, "worker1")
    assert job1.job_id == "1"
    assert job1.callback_url == FINISH_JOB_CALLBACK_URL
    assert job_handler.get_pending_jobs_num(rclient) == 2
    assert job_handler.get_assigned_jobs_num(rclient) == 1

    job2 = job_handler.handle_take_job(rclient, "worker2")
    assert job2.job_id == "2"
    assert job2.callback_url == FINISH_JOB_CALLBACK_URL
    assert job_handler.get_pending_jobs_num(rclient) == 1
    assert job_handler.get_assigned_jobs_num(rclient) == 2

    job3 = job_handler.handle_take_job(rclient, "worker3")
    assert job3.job_id == "3"
    assert job3.callback_url == FINISH_JOB_CALLBACK_URL
    assert job_handler.get_pending_jobs_num(rclient) == 0
    assert job_handler.get_assigned_jobs_num(rclient) == 3

    # check processing job queue
    assert job_handler._get_assigned_job(rclient, "1").worker == "worker1"
    assert job_handler._get_assigned_job(rclient, "2").worker == "worker2"
    assert job_handler._get_assigned_job(rclient, "3").worker == "worker3"


def test_take_job_error():
    rclient = RedisMock()
    with pytest.raises(ValueError) as e1:
        job_handler.handle_take_job(rclient, "worker1")
    assert e1.match("no job available")

    _add_new_jobs(rclient, 3)
    job_handler._block_worker(rclient, "worker1")
    with pytest.raises(ValueError) as e2:
        job_handler.handle_take_job(rclient, "worker1")
    assert e2.match("worker is blocked")


def test_finish_job_ok():
    rclient = RedisMock()
    mdb = MongoMock()
    _add_new_jobs(rclient, 3)
    job_handler.handle_take_job(rclient, "worker1")
    job_handler.handle_take_job(rclient, "worker2")
    job_handler.handle_take_job(rclient, "worker3")

    # Case 1: job 1 finished by worker1
    r1 = WorkerJobResult(job_id="1", worker="worker1", output=["t1"])
    job_handler.handle_finish_job(rclient, mdb, r1)
    assert job_handler.get_assigned_jobs_num(rclient) == 2
    j1 = mdb.find_one({"_id": "1"})
    assert j1["output"] == ["t1"]
    assert j1["worker"] == "worker1"
    assert j1["finished_at"] is not None
    assert j1["publisher"] == "p1"

    # Case 2: job 2 finished by worker2
    r2 = WorkerJobResult(job_id="2", worker="worker2", output=["t2"])
    job_handler.handle_finish_job(rclient, mdb, r2)
    assert job_handler.get_assigned_jobs_num(rclient) == 1
    j2 = mdb.find_one({"_id": "2"})
    assert j2["output"] == ["t2"]
    assert j2["worker"] == "worker2"
    assert j2["finished_at"] is not None
    assert j2["publisher"] == "p2"

    # Case 3: job 3 finished by worker3
    r3 = WorkerJobResult(job_id="3", worker="worker3", output=["t3"])
    job_handler.handle_finish_job(rclient, mdb, r3)
    assert job_handler.get_assigned_jobs_num(rclient) == 0
    j3 = mdb.find_one({"_id": "3"})
    assert j3["output"] == ["t3"]
    assert j3["worker"] == "worker3"
    assert j3["finished_at"] is not None
    assert j3["publisher"] == "p3"


def test_finish_job_verify(mocker):
    rclient = RedisMock()
    mdb = MongoMock()
    _add_new_jobs(rclient, 3)

    job_handler.handle_take_job(rclient, "worker1")
    job_handler.handle_take_job(rclient, "worker2")

    r1 = WorkerJobResult(job_id="1", worker="worker1", output=["t1"])
    mocker.patch("mizu_node.job_handler._should_verify", return_value=False)
    with patch("mizu_node.job_handler._request_verify_job") as mocked_function:
        job_handler.handle_finish_job(rclient, mdb, r1)
        mocked_function.assert_not_called()

    r2 = WorkerJobResult(job_id="2", worker="worker2", output=["t2"])
    mocker.patch("mizu_node.job_handler._should_verify", return_value=True)
    with patch("mizu_node.job_handler._request_verify_job") as mocked_function:
        assigned = job_handler._get_assigned_job(rclient, r2.job_id)
        job_handler.handle_finish_job(rclient, mdb, r2)
        job = WorkerJob.from_pending_job(assigned, VERIFY_JOB_CALLBACK_URL)
        job_handler._request_verify_job.assert_called_once_with(job)


def test_finish_job_error():
    rclient = RedisMock()
    mdb = MongoMock()
    _add_new_jobs(rclient, 3)

    job_handler._add_assigned_job(
        rclient,
        "1",
        AssignedJob(
            job_id="1",
            job_type=JobType.classification,
            publisher="p1",
            published_at=epoch() - 7200,
            input="1",
            worker="worker1",
            assigned_at=epoch() - 4800,
        ).model_dump_json(),
    )

    r1 = WorkerJobResult(job_id="1", worker="worker2", output=["t1"])
    with pytest.raises(ValueError) as e1:
        job_handler.handle_finish_job(rclient, mdb, r1)
    assert e1.match("worker mismatch")

    r2 = WorkerJobResult(job_id="1", worker="worker1", output=["t1"])
    with pytest.raises(ValueError) as e2:
        job_handler.handle_finish_job(rclient, mdb, r2)
    assert e2.match("job expired")


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
        assigned_at=epoch() - 4800,
        finished_at=epoch() - 3600,
        output=["t1", "t2"],
    )
    job_handler._save_finished_job(mdb, jresult)
    assert not job_handler._is_worker_blocked(rclient, "worker1")

    result = WorkerJobResult(job_id="1", worker="worker1", output=["t1", "t2"])
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
        assigned_at=epoch() - 4800,
        finished_at=epoch() - 3600,
        output=["t1", "t2"],
    )
    job_handler._save_finished_job(mdb, jresult)
    assert not job_handler._is_worker_blocked(rclient, "worker1")

    result = WorkerJobResult(job_id="1", worker="worker1", output=["t2"])
    job_handler.handle_verify_job_result(rclient, mdb, result)
    assert job_handler._is_worker_blocked(rclient, "worker1")


def test_verify_job_error():
    rclient = RedisMock()
    mdb = MongoMock()

    result = WorkerJobResult(job_id="1", worker="worker1", output=["t2"])
    with pytest.raises(ValueError) as e:
        job_handler.handle_verify_job_result(rclient, mdb, result)
    assert e.match("invalid job")
