import asyncio
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, Security, status, Depends
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pymongo import MongoClient
import redis

from mizu_node.error_handler import error_handler
from mizu_node.constants import (
    FINISHED_JOBS_COLLECTIONS,
    MONGO_DB_NAME,
    MONGO_URL,
    REDIS_URL,
    COOLDOWN_WORKER_EXPIRE_TTL_SECONDS,
    VERIFY_KEY,
)
from mizu_node.job_handler import (
    handle_queue_len,
    handle_take_job,
    handle_publish_jobs,
    handle_finish_job,
    queue_clean,
    handle_queue_len,
)
from mizu_node.security import verify_jwt, verify_api_key
from mizu_node.types.common import JobType
from mizu_node.types.data_job import PublishJobRequest, WorkerJobResult
from mizu_node.utils import build_json_response
from mizu_node.worker_handler import has_worker_cooled_down

rclient = redis.Redis.from_url(REDIS_URL, decode_responses=True)
mclient = MongoClient(MONGO_URL)
mdb = mclient[MONGO_DB_NAME]

# Security scheme
bearer_scheme = HTTPBearer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, queue_clean, rclient)
    yield


app = FastAPI(lifespan=lifespan)


def get_user(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> str:
    token = credentials.credentials
    return verify_jwt(token, VERIFY_KEY)


def get_publisher(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> str:
    token = credentials.credentials
    return verify_api_key(mdb, token)


@app.get("/")
@app.get("/healthcheck")
def default():
    return {"status": "ok"}


@app.post("/publish_jobs")
@error_handler
def publish_jobs(req: PublishJobRequest, publisher: str = Depends(get_publisher)):
    # TODO: ensure it's called from whitelisted publisher
    ids = handle_publish_jobs(rclient, publisher, req)
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "ok", "data": {"job_ids": ids}},
    )


@app.get("/take_job")
@error_handler
def take_job(job_type: JobType, user: str = Depends(get_user)):
    if not has_worker_cooled_down(rclient, user):
        message = f"please retry after ${COOLDOWN_WORKER_EXPIRE_TTL_SECONDS}"
        return build_json_response(status.HTTP_429_TOO_MANY_REQUESTS, message)
    job = handle_take_job(rclient, user, job_type)
    if job is None:
        return build_json_response(
            status.HTTP_200_OK, "no job available", {"job": None}
        )
    else:
        return build_json_response(status.HTTP_200_OK, "ok", {"job": job.model_dump()})


@app.post("/finish_job")
@error_handler
def finish_job(job: WorkerJobResult, user: str = Depends(get_user)):
    handle_finish_job(rclient, mdb[FINISHED_JOBS_COLLECTIONS], user, job)
    return build_json_response(status.HTTP_200_OK, "ok")


@app.get("/queue_len")
@error_handler
def queue_len(job_type: JobType = JobType.classify):
    """
    Return the number of queued classify jobs.
    """
    q_len = handle_queue_len(rclient, job_type)
    return build_json_response(status.HTTP_200_OK, "ok", {"length": q_len})


def start_dev():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000, reload=True)


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000)
