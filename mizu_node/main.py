import os
from bson import ObjectId
import uvicorn
from fastapi import FastAPI, Security, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pymongo import MongoClient
import redis

from mizu_node.error_handler import error_handler
from mizu_node.constants import (
    API_KEY_COLLECTION,
    CLASSIFIER_COLLECTION,
    JOBS_COLLECTION,
    MIZU_NODE_MONGO_DB_NAME,
    REDIS_URL,
    COOLDOWN_WORKER_EXPIRE_TTL_SECONDS,
)
from mizu_node.job_handler import (
    handle_query_job,
    handle_take_job,
    handle_publish_jobs,
    handle_finish_job,
    handle_queue_len,
)
from mizu_node.security import verify_jwt, verify_api_key
from mizu_node.types.classifier import ClassifierConfig
from mizu_node.types.job import (
    JobType,
    PublishJobRequest,
    QueryJobRequest,
    WorkerJobResult,
)
from mizu_node.utils import build_json_response
from mizu_node.worker_handler import has_worker_cooled_down

# Security scheme
bearer_scheme = HTTPBearer()

app = FastAPI()
app.rclient = redis.Redis.from_url(REDIS_URL, decode_responses=True)
app.mclient = MongoClient(os.environ["MIZU_NODE_MONGO_URL"])
app.mdb = app.mclient[MIZU_NODE_MONGO_DB_NAME]


def get_user(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> str:
    token = credentials.credentials
    return verify_jwt(token, os.environ["JWT_VERIFY_KEY"])


def get_publisher(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> str:
    token = credentials.credentials
    return verify_api_key(app.mdb[API_KEY_COLLECTION], token)


@app.get("/")
@app.get("/healthcheck")
def default():
    return {"status": "ok"}


@app.post("/register_classifier")
@error_handler
def register_classifier(
    classifier: ClassifierConfig, publisher: str = Depends(get_publisher)
):
    classifier.publisher = publisher
    result = app.mdb[CLASSIFIER_COLLECTION].insert_one(
        classifier.model_dump(by_alias=True)
    )
    return build_json_response(
        status.HTTP_200_OK, "ok", {"id": str(result.inserted_id)}
    )


@app.get("/classifier_info")
@error_handler
def get_classifier(id: str):
    doc = app.mdb[CLASSIFIER_COLLECTION].find_one({"_id": ObjectId(id)})
    if doc is None:
        return build_json_response(status.HTTP_404_NOT_FOUND, "classifier not found")
    return build_json_response(
        status.HTTP_200_OK,
        "ok",
        {"classifier": ClassifierConfig(**doc).model_dump(by_alias=True)},
    )


@app.post("/publish_jobs")
@error_handler
def publish_jobs(req: PublishJobRequest, publisher: str = Depends(get_publisher)):
    ids = list(handle_publish_jobs(app.mdb, publisher, req))
    return build_json_response(status.HTTP_200_OK, "ok", {"jobIds": ids})


@app.post("/job_status")
@error_handler
def query_job_status(req: QueryJobRequest, publisher: str = Depends(get_publisher)):
    jobs = handle_query_job(app.mdb[JOBS_COLLECTION], publisher, req)
    return build_json_response(
        status.HTTP_200_OK, "ok", {"jobs": [j.model_dump(by_alias=True) for j in jobs]}
    )


@app.get("/take_job")
@error_handler
def take_job(
    job_type: JobType,
    user: str = Depends(get_user),
):
    if not has_worker_cooled_down(app.rclient, user):
        message = f"please retry after ${COOLDOWN_WORKER_EXPIRE_TTL_SECONDS}"
        return build_json_response(status.HTTP_429_TOO_MANY_REQUESTS, message)
    job = handle_take_job(app.rclient, user, job_type)
    if job is None:
        return build_json_response(
            status.HTTP_200_OK, "no job available", {"job": None}
        )
    else:
        return build_json_response(
            status.HTTP_200_OK, "ok", {"job": job.model_dump(by_alias=True)}
        )


@app.post("/finish_job")
@error_handler
def finish_job(job: WorkerJobResult, user: str = Depends(get_user)):
    handle_finish_job(app.rclient, app.mdb[JOBS_COLLECTION], user, job)
    return build_json_response(status.HTTP_200_OK, "ok")


@app.get("/stats/queue_len")
@error_handler
def queue_len(job_type: JobType = JobType.pow):
    """
    Return the number of queued classify jobs.
    """
    q_len = handle_queue_len(job_type)
    return build_json_response(status.HTTP_200_OK, "ok", {"length": q_len})


def start_dev():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000, reload=True)


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run("mizu_node.main:app", host=["::", "0.0.0.0"], port=8000)
