import uvicorn
from fastapi import FastAPI


from mizu_node import error_handler
from mizu_node.db.job import (
    ClassificationJobFromPublisher,
    ClassificationJobResult,
    handle_take_job,
    handle_new_job,
    handle_finish_job,
    handle_verify_job_result,
    get_pending_jobs_num,
    get_processing_jobs_num,
)

app = FastAPI()


def get_caller() -> str:
    return ""


@app.get("/")
@app.get("/healthcheck")
async def default():
    return {"status": "ok"}


@app.get("stat/pending_jobs_len")
@error_handler
async def pending_jobs_len():
    return get_pending_jobs_num()


@app.get("stat/assigned_jobs_len")
@error_handler
async def assigned_jobs_len():
    return get_processing_jobs_num()


@app.get("take_job")
@error_handler
async def take_job():
    job_for_worker = handle_take_job(get_caller())
    return {"job": job_for_worker.model_dump_json()}


@app.post("add_job")
@error_handler
async def add_job(jobs: list[ClassificationJobFromPublisher]):
    handle_new_job(jobs)
    return {"status": "ok"}


@app.post("finish_job")
@error_handler
async def finish_job(job: ClassificationJobResult):
    handle_finish_job(job)
    return {"status": "ok"}


# this job can only be called by validator
@app.post("verify_job_callback")
@error_handler
async def verify_job(job: ClassificationJobResult):
    handle_verify_job_result(job)
    return {"status": "ok"}


def start_dev():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000, reload=True)


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000)
