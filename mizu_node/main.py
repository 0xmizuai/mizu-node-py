import asyncio
from contextlib import asynccontextmanager
import logging
import os
from typing import List
from bson import ObjectId
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Security, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import redis

from mizu_node.common import build_ok_response, error_handler
from mizu_node.constants import (
    API_KEY_COLLECTION,
    CLASSIFIER_COLLECTION,
    JOBS_COLLECTION,
    MIZU_NODE_MONGO_DB_NAME,
    REDIS_URL,
)
from mizu_node.job_handler import (
    handle_query_job,
    handle_take_job,
    handle_publish_jobs,
    handle_finish_job,
    handle_queue_len,
    validate_admin_job,
    validate_classifiers,
)
from mizu_node.security import (
    get_allowed_origins,
    validate_worker,
    verify_jwt,
    verify_api_key,
)
from mizu_node.stats import (
    get_valid_rewards,
    total_mined_points_in_past_n_days,
    total_mined_points_in_past_n_days_per_worker,
    total_mined_points_in_past_n_hour,
    total_mined_points_in_past_n_hour_per_worker,
    total_rewarded_in_past_n_days,
    total_rewarded_in_past_n_hour,
)
from mizu_node.types.classifier import ClassifierConfig
from mizu_node.types.data_job import JobType
from mizu_node.types.service import (
    FinishJobRequest,
    FinishJobResponse,
    PublishBatchClassifyJobRequest,
    PublishJobResponse,
    PublishPowJobRequest,
    PublishRewardJobRequest,
    QueryClassifierResponse,
    QueryJobResponse,
    QueryMinedPointsResponse,
    QueryQueueLenResponse,
    RegisterClassifierRequest,
    RegisterClassifierResponse,
    TakeJobResponse,
)
from mizu_node.types.job_queue import queue_clean, queue_clear

logging.basicConfig(level=logging.INFO)  # Set the desired logging level

# Security scheme
bearer_scheme = HTTPBearer()
rclient = redis.Redis.from_url(REDIS_URL, decode_responses=True)
logging.info(f"Connected to redis at {REDIS_URL}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, queue_clean, rclient)
    yield


app = FastAPI(lifespan=lifespan)
origins = get_allowed_origins()
logging.info(f"allowed origins are {origins}")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.rclient = rclient
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
    request: RegisterClassifierRequest, publisher: str = Depends(get_publisher)
):
    request.config.publisher = publisher
    result = app.mdb[CLASSIFIER_COLLECTION].insert_one(
        request.config.model_dump(by_alias=True)
    )
    response = RegisterClassifierResponse(id=str(result.inserted_id))
    return build_ok_response(response)


@app.get("/classifier_info")
@error_handler
def get_classifier(id: str):
    doc = app.mdb[CLASSIFIER_COLLECTION].find_one({"_id": ObjectId(id)})
    if doc is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="classifier not found"
        )
    response = QueryClassifierResponse(classifier=ClassifierConfig(**doc))
    return build_ok_response(response)


@app.post("/clear_queue")
@error_handler
def clear_queue(job_type: JobType, publisher: str = Depends(get_publisher)):
    validate_admin_job(publisher)
    queue_clear(app.rclient, job_type)
    return build_ok_response()


@app.post("/publish_pow_jobs")
@error_handler
def publish_pow_jobs(
    request: PublishPowJobRequest,
    publisher: str = Depends(get_publisher),
):
    validate_admin_job(publisher)
    ids = handle_publish_jobs(
        app.rclient, app.mdb, publisher, JobType.pow, request.data
    )
    return build_ok_response(PublishJobResponse(job_ids=ids))


@app.post("/publish_reward_jobs")
@error_handler
def publish_pow_jobs(
    request: PublishRewardJobRequest,
    publisher: str = Depends(get_publisher),
):
    validate_admin_job(publisher)
    ids = handle_publish_jobs(
        app.rclient, app.mdb, publisher, JobType.reward, request.data
    )
    return build_ok_response(PublishJobResponse(job_ids=ids))


@app.post("/publish_batch_classify_jobs")
@error_handler
def publish_jobs(
    request: PublishBatchClassifyJobRequest, publisher: str = Depends(get_publisher)
):
    validate_classifiers(app.mdb, request.data)
    ids = handle_publish_jobs(
        app.rclient, app.mdb, publisher, JobType.batch_classify, request.data
    )
    return build_ok_response(PublishJobResponse(job_ids=ids))


@app.get("/job_status")
@error_handler
def query_job_status(ids: List[str] = Query(None), _: str = Depends(get_publisher)):
    jobs = handle_query_job(app.mdb[JOBS_COLLECTION], ids)
    return build_ok_response(QueryJobResponse(jobs=jobs))


@app.get("/reward_jobs")
@error_handler
def query_reward_jobs(user: str = Depends(get_user)):
    rewards = get_valid_rewards(app.rclient, user)
    return build_ok_response(rewards)


@app.get("/take_job")
@error_handler
def take_job(
    job_type: JobType,
    user: str = Depends(get_user),
):
    validate_worker(app.rclient, user, job_type)
    job = handle_take_job(app.rclient, app.mdb[JOBS_COLLECTION], user, job_type)
    return build_ok_response(TakeJobResponse(job=job))


@app.post("/finish_job")
@error_handler
def finish_job(request: FinishJobRequest, user: str = Depends(get_user)):
    points = handle_finish_job(
        app.rclient, app.mdb[JOBS_COLLECTION], user, request.job_result
    )
    return build_ok_response(FinishJobResponse(rewarded_points=points))


@app.get("/stats/queue_len")
@app.get("/global_stats/queue_len")
@error_handler
def queue_len(job_type: JobType = JobType.pow):
    """
    Return the number of queued classify jobs.
    """
    q_len = handle_queue_len(app.rclient, job_type)
    return build_ok_response(QueryQueueLenResponse(length=q_len))


@app.get("/global_stats/mined_points")
@error_handler
def get_mined_points_stats(hours: int | None = None, days: int | None = None):
    """
    Return the mined points in the last `hours` hours or last `days` days.
    """
    if hours is None and days is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="either hours or days must be provided",
        )
    if hours is not None:
        points = total_mined_points_in_past_n_hour(app.rclient, max(hours, 24))
    if days is not None:
        points = total_mined_points_in_past_n_days(app.rclient, max(days, 7))
    return build_ok_response(QueryMinedPointsResponse(points=points))


@app.get("/global_stats/rewards")
@error_handler
def get_rewards_stats(token: str, hours: int | None = None, days: int | None = None):
    """
    Return the mined points in the last `hours` hours or last `days` days.
    """
    if hours is None and days is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="either hours or days must be provided",
        )
    if hours is not None:
        points = total_rewarded_in_past_n_hour(app.rclient, token, max(hours, 24))
    if days is not None:
        points = total_rewarded_in_past_n_days(app.rclient, token, max(days, 7))
    return build_ok_response(QueryMinedPointsResponse(points=points))


@app.get("/stats/mined_points")
@app.get("/worker_stats/mined_points")
@error_handler
def get_mined_points(
    hours: int | None = None, days: int | None = None, user=Depends(get_user)
):
    """
    Return the mined points in the last `hours` hours or last `days` days.
    """
    if hours is None and days is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="either hours or days must be provided",
        )
    if hours is not None:
        points = total_mined_points_in_past_n_hour_per_worker(
            app.rclient, user, max(hours, 24)
        )
    if days is not None:
        points = total_mined_points_in_past_n_days_per_worker(
            app.rclient, user, max(days, 7)
        )
    return build_ok_response(QueryMinedPointsResponse(points=points))


class MyServer(uvicorn.Server):
    async def run(self, sockets=None):
        self.config.setup_event_loop()
        return await self.serve(sockets=sockets)


async def run():
    apps = []
    config1 = uvicorn.Config("mizu_node.dummy_service:app", host="0.0.0.0", port=8001)
    config2 = uvicorn.Config(
        "mizu_node.main:app", host="0.0.0.0", port=8000, lifespan="on", reload=True
    )
    apps.append(MyServer(config=config1).run())
    apps.append(MyServer(config=config2).run())
    return await asyncio.gather(*apps)


def start_dev():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run("mizu_node.main:app", host=["::", "0.0.0.0"], lifespan="on", port=8000)
