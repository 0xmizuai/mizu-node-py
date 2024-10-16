from unittest.mock import patch
import pytest

from mizu_node.constants import (
    FINISH_JOB_CALLBACK_URL,
    VERIFY_JOB_CALLBACK_URL,
)
from mizu_node.types import (
    JobType,
    AssignedJob,
    FinishedJob,
    PendingJobPayload,
    WorkerJob,
    WorkerJobResult,
)
import mizu_node.job_handler as job_handler
from mizu_node.utils import epoch
from tests.mongo_mock import MongoMock
from tests.redis_mock import RedisMock


def _new_pending_job(key: str, job_type: JobType):
    return PendingJobPayload(
        job_type=job_type,
        publisher="p",
        published_at=epoch(),
        input=key,
    )


def _add_new_jobs(rclient, job_type: JobType, num_jobs=3):
    jobs = [_new_pending_job(str(i + 1), job_type) for i in range(num_jobs)]
    return job_handler.handle_publish_jobs(rclient, jobs)


def test_publish_jobs():
    rclient = RedisMock()
    _add_new_jobs(rclient, JobType.classification, 3)
    assert job_handler.get_pending_jobs_num(rclient) == 3
    assert job_handler.get_pending_jobs_num(rclient, JobType.classification) == 3
    assert job_handler.get_pending_jobs_num(rclient, JobType.pow) == 0

    _add_new_jobs(rclient, JobType.pow, 3)
    assert job_handler.get_pending_jobs_num(rclient) == 6
    assert job_handler.get_pending_jobs_num(rclient, JobType.classification) == 3
    assert job_handler.get_pending_jobs_num(rclient, JobType.pow) == 3


def test_take_job_ok():
    rclient = RedisMock()
    pids = _add_new_jobs(rclient, JobType.pow, 3)
    cids = _add_new_jobs(rclient, JobType.classification, 3)
    assert job_handler.get_pending_jobs_num(rclient) == 6
    assert job_handler.get_pending_jobs_num(rclient, JobType.pow) == 3
    assert job_handler.get_pending_jobs_num(rclient, JobType.classification) == 3

    # take classification job 1
    job1 = job_handler.handle_take_job(rclient, "worker1", [JobType.classification])
    assert job1.job_id == cids[0]
    assert job1.job_type == JobType.classification
    assert job1.callback_url == FINISH_JOB_CALLBACK_URL

    assert job_handler.get_pending_jobs_num(rclient) == 5
    assert job_handler.get_pending_jobs_num(rclient, JobType.pow) == 3
    assert job_handler.get_pending_jobs_num(rclient, JobType.classification) == 2
    assert job_handler.get_assigned_jobs_num(rclient) == 1

    # take pow job 1
    job2 = job_handler.handle_take_job(rclient, "worker2", [JobType.pow])
    assert job2.job_id == pids[0]
    assert job2.job_type == JobType.pow
    assert job2.callback_url == FINISH_JOB_CALLBACK_URL

    assert job_handler.get_pending_jobs_num(rclient) == 4
    assert job_handler.get_pending_jobs_num(rclient, JobType.pow) == 2
    assert job_handler.get_pending_jobs_num(rclient, JobType.classification) == 2
    assert job_handler.get_assigned_jobs_num(rclient) == 2

    # take classification job 2
    job3 = job_handler.handle_take_job(rclient, "worker3")
    assert job3.job_id == cids[1]
    assert job3.job_type == JobType.classification
    assert job3.callback_url == FINISH_JOB_CALLBACK_URL

    assert job_handler.get_pending_jobs_num(rclient) == 3
    assert job_handler.get_pending_jobs_num(rclient, JobType.pow) == 2
    assert job_handler.get_pending_jobs_num(rclient, JobType.classification) == 1
    assert job_handler.get_assigned_jobs_num(rclient) == 3

    # check processing job queue
    assert job_handler._get_assigned_job(rclient, job1.job_id).worker == "worker1"
    assert job_handler._get_assigned_job(rclient, job2.job_id).worker == "worker2"
    assert job_handler._get_assigned_job(rclient, job3.job_id).worker == "worker3"


def test_take_job_error():
    rclient = RedisMock()
    with pytest.raises(ValueError) as e1:
        job_handler.handle_take_job(rclient, "worker1")
    assert e1.match("no job available")

    _add_new_jobs(rclient, JobType.pow, 3)
    with pytest.raises(ValueError) as e1:
        job_handler.handle_take_job(rclient, "worker1", [JobType.classification])
    assert e1.match("no job available")

    job_handler._block_worker(rclient, "worker1")
    with pytest.raises(ValueError) as e2:
        job_handler.handle_take_job(rclient, "worker1")
    assert e2.match("worker is blocked")


def test_finish_job_ok(mocker):
    rclient = RedisMock()
    mdb = MongoMock()
    cids = _add_new_jobs(rclient, JobType.classification, 3)
    pids = _add_new_jobs(rclient, JobType.pow, 3)

    job_handler.handle_take_job(rclient, "worker1", [JobType.classification])
    job_handler.handle_take_job(rclient, "worker2", [JobType.pow])
    mocker.patch("mizu_node.job_handler._request_verify_job", return_value=None)

    # Case 1: job 1 finished by worker1
    r1 = WorkerJobResult(job_id=cids[0], worker="worker1", output=["t1"])
    job_handler.handle_finish_job(rclient, mdb, r1)
    j1 = mdb.find_one({"_id": cids[0]})
    assert j1["output"] == ["t1"]
    assert j1["worker"] == "worker1"
    assert j1["finished_at"] is not None
    assert j1["job_type"] == JobType.classification

    # Case 2: job 2 finished by worker2
    r2 = WorkerJobResult(job_id=pids[0], worker="worker2", output=["t2"])
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

    r1 = WorkerJobResult(job_id=cids[0], worker="worker1", output=["t1"])
    mocker.patch("mizu_node.job_handler._should_verify", return_value=False)
    with patch("mizu_node.job_handler._request_verify_job") as mocked_function:
        job_handler.handle_finish_job(rclient, mdb, r1)
        mocked_function.assert_not_called()

    r2 = WorkerJobResult(job_id=pids[0], worker="worker2", output=["t2"])
    mocker.patch("mizu_node.job_handler._should_verify", return_value=True)
    with patch("mizu_node.job_handler._request_verify_job") as mocked_function:
        assigned = job_handler._get_assigned_job(rclient, r2.job_id)
        job_handler.handle_finish_job(rclient, mdb, r2)
        job = WorkerJob.from_pending_job(assigned, VERIFY_JOB_CALLBACK_URL)
        job_handler._request_verify_job.assert_called_once_with(job)


def test_finish_job_error():
    rclient = RedisMock()
    mdb = MongoMock()
    cids = _add_new_jobs(rclient, JobType.classification, 3)

    job_handler._add_assigned_job(
        rclient,
        AssignedJob(
            job_id=cids[0],
            job_type=JobType.classification,
            publisher="p",
            published_at=epoch() - 7200,
            input="1",
            worker="worker1",
            assigned_at=epoch() - 4800,
        ),
    )

    r1 = WorkerJobResult(job_id=cids[0], worker="worker2", output=["t1"])
    with pytest.raises(ValueError) as e1:
        job_handler.handle_finish_job(rclient, mdb, r1)
    assert e1.match("worker mismatch")

    r2 = WorkerJobResult(job_id=cids[0], worker="worker1", output=["t1"])
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
