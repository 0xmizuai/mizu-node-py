import pytest
from pytest_mock_resources import create_redis_fixture, create_mongo_fixture

from mizu_node.constants import FINISH_JOB_CALLBACK_URL
from mizu_node.types import ProcessingJob
from mizu_node.types import ClassificationJobResult
from mizu_node.types import ClassificationJobFromPublisher
import mizu_node.job_handler as job_handler


def _new_classification_job(_id: str):
    return ClassificationJobFromPublisher(
        _id=_id, publisher="p" + _id, created_at=job_handler.now()
    )


def prepare():
    job_handler.rclient = create_redis_fixture()
    job_handler.mdb = create_mongo_fixture()["job"]
    return [
        _new_classification_job("1"),
        _new_classification_job("2"),
        _new_classification_job("3"),
    ]


def test_new_job():
    # Given
    jobs = prepare()

    # When
    job_handler.handle_new_job(jobs)

    # Then
    assert job_handler.get_pending_jobs_num() == 3
    job_handler.rclient.get("pending_jobs_queue").decode() == "1,2,3"


def test_take_job_ok():
    jobs = prepare()
    job_handler.handle_new_job(jobs)

    job1 = job_handler.handle_take_job("worker1")
    assert job1._id == "1"
    assert job1.callback_url == FINISH_JOB_CALLBACK_URL
    assert job_handler.get_pending_jobs_num() == 2
    assert job_handler.get_processing_jobs_num() == 1

    job2 = job_handler.handle_take_job("worker2")
    assert job2._id == "2"
    assert job2.callback_url == FINISH_JOB_CALLBACK_URL
    assert job_handler.get_pending_jobs_num() == 1
    assert job_handler.get_processing_jobs_num() == 2

    job3 = job_handler.handle_take_job("worker3")
    assert job3._id == "3"
    assert job3.callback_url == FINISH_JOB_CALLBACK_URL
    assert job_handler.get_pending_jobs_num() == 0
    assert job_handler.get_processing_jobs_num() == 3

    # check processing job queue
    assert job_handler._get_processing_job("1").worker == "worker1"
    assert job_handler._get_processing_job("2").worker == "worker2"
    assert job_handler._get_processing_job("3").worker == "worker3"


def test_take_job_error():
    with pytest.raises(ValueError) as e1:
        job_handler.handle_take_job("worker1")
    assert e1.match("no job available")

    jobs = prepare()
    job_handler.handle_new_job(jobs)
    job_handler._block_worker("worker1")
    with pytest.raises(ValueError) as e2:
        job_handler.handle_take_job("worker1")
    assert e2.match("worker is blocked")


def test_finish_job_ok():
    job_handler.handle_new_job(prepare())
    job_handler.handle_take_job("worker1")
    job_handler.handle_take_job("worker2")
    job_handler.handle_take_job("worker3")

    r1 = ClassificationJobResult("1", "worker1", ["t1"])
    job_handler.handle_finish_job(r1)
    assert job_handler.get_processing_jobs_num() == 2

    r2 = ClassificationJobResult("2", "worker2", ["t2"])
    job_handler.handle_finish_job(r2)

    assert job_handler.get_processing_jobs_num() == 1
    r3 = ClassificationJobResult("3", "worker3", ["t3"])
    job_handler.handle_finish_job(r3)
    assert job_handler.get_processing_jobs_num() == 0


def test_finish_job_error():
    processing_job = ProcessingJob(
        _id=1,
        publisher="p1",
        created_at=job_handler.now() - 7200,
        worker="worker1",
        assigned_at=job_handler.now() - 4800,
    )
    job_handler._add_processing_job(
        job_handler.rclient,
        "1",
        processing_job.model_dump_json(),
    )

    r1 = ClassificationJobResult("1", "worker2", ["t1"])
    with pytest.raises(ValueError) as e1:
        job_handler.handle_finish_job(r1)
    assert e1.match("worker mismatch")

    r2 = ClassificationJobResult("1", "worker1", ["t1"])
    with pytest.raises(ValueError) as e2:
        job_handler.handle_finish_job(r2)
    assert e2.match("job expired")
