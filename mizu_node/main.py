import uvicorn
from fastapi import FastAPI
from pymongo import MongoClient
import redis

from mizu_node.error_handler import error_handler
from mizu_node.constants import MONGO_DB_NAME, MONGO_URL, REDIS_URL
from mizu_node.job_handler import (
    ClassificationJobFromPublisher,
    ClassificationJobResult,
    handle_take_job,
    handle_new_jobs,
    handle_finish_job,
    handle_verify_job_result,
    get_pending_jobs_num,
    get_processing_jobs_num,
)


app = FastAPI()
rclient = redis.Redis(REDIS_URL)
mdb = MongoClient(MONGO_URL)[MONGO_DB_NAME]


def get_user() -> str:
    return ""


@app.get("/")
@app.get("/healthcheck")
async def default():
    return {"status": "ok"}


@app.get("stat/pending_jobs_len")
@error_handler
async def pending_jobs_len():
    return get_pending_jobs_num(rclient)


@app.get("stat/assigned_jobs_len")
@error_handler
async def assigned_jobs_len():
    return get_processing_jobs_num(rclient)


@app.get("take_job")
@error_handler
async def take_job():
    # TODO: add rate limit and cool down
    job = handle_take_job(rclient, get_user())
    return {"job": job.model_dump_json()}


@app.post("add_job")
@error_handler
async def add_job(jobs: list[ClassificationJobFromPublisher]):
    # TODO: ensure it's called from whitelisted publisher
    handle_new_jobs(rclient, jobs)
    return {"status": "ok"}


@app.post("finish_job")
@error_handler
async def finish_job(job: ClassificationJobResult):
    job.worker = get_user()
    handle_finish_job(rclient, mdb, job)
    return {"status": "ok"}


@app.post("verify_job_callback")
@error_handler
async def verify_job(job: ClassificationJobResult):
    # TODO: ensure it's called from validator
    handle_verify_job_result(mdb, job)
    return {"status": "ok"}


def start_dev():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000, reload=True)


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000)
