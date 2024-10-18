from asyncio import sleep
import asyncio
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pymongo import MongoClient
import redis

from mizu_node.error_handler import error_handler
from mizu_node.constants import (
    MONGO_DB_NAME,
    MONGO_URL,
    REDIS_URL,
    COOLDOWN_WORKER_EXPIRE_TTL_SECONDS,
)
from mizu_node.job_handler import (
    WorkerJobResult,
    handle_take_job,
    handle_publish_jobs,
    handle_finish_job,
    handle_verify_job_result,
)
from mizu_node.types import PublishJobRequest
from mizu_node.worker_handler import has_worker_cooled_down

from mizu_node.job_queue import job_queues

rclient = redis.Redis.from_url(REDIS_URL)
mclient = MongoClient(MONGO_URL)
mdb = mclient[MONGO_DB_NAME]


@asynccontextmanager
async def lifespan(app: FastAPI):
    def db_clean():
        while True:
            for queue in job_queues.values():
                queue.light_clean(rclient)
            sleep(60)

    asyncio.create_task(asyncio.coroutine(db_clean)())
    yield


app = FastAPI(lifespan=lifespan)


def get_user() -> str:
    return ""


@app.get("/")
@app.get("/healthcheck")
async def default():
    return {"status": "ok"}


@app.post("publish_jobs")
@error_handler
async def publish_jobs(req: PublishJobRequest):
    # TODO: ensure it's called from whitelisted publisher
    ids = handle_publish_jobs(rclient, req)
    return {"ids": ids}


@app.get("take_job")
@error_handler
async def take_job():
    user = get_user()
    if not has_worker_cooled_down(rclient, user):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content=jsonable_encoder({"cool_down": COOLDOWN_WORKER_EXPIRE_TTL_SECONDS}),
        )
    job = handle_take_job(rclient, user)
    return {"job": job.model_dump_json()}


@app.post("finish_job")
@error_handler
async def finish_job(job: WorkerJobResult):
    job.worker = get_user()
    handle_finish_job(rclient, mdb, job)
    return {"status": "ok"}


@app.post("verify_job_callback")
@error_handler
async def verify_job(job: WorkerJobResult):
    # TODO: ensure it's called from validator
    handle_verify_job_result(rclient, mdb, job)
    return {"status": "ok"}


def start_dev():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000, reload=True)


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000)
