import asyncio
from contextlib import asynccontextmanager
import logging
import os
from typing import List
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, Security, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware

from prometheus_client import Counter, Histogram, make_asgi_app

from mizu_node.common import build_ok_response, epoch_ms, error_handler
from mizu_node.constants import (
    LATENCY_BUCKETS,
)
from mizu_node.job_handler import (
    handle_finish_job_v2,
    handle_query_job,
    handle_take_job,
    handle_publish_jobs,
    handle_finish_job,
    handle_queue_len,
    validate_admin_job,
)
from mizu_node.security import (
    get_allowed_origins,
    verify_jwt,
    verify_api_key,
)
from mizu_node.stats import (
    total_mined_points_in_past_n_days,
    total_mined_points_in_past_n_days_per_worker,
    total_mined_points_in_past_n_hour,
    total_mined_points_in_past_n_hour_per_worker,
    total_rewarded_in_past_n_days,
    total_rewarded_in_past_n_hour,
)
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import JobType
from mizu_node.types.service import (
    FinishJobRequest,
    FinishJobResponse,
    FinishJobV2Response,
    PublishBatchClassifyJobRequest,
    PublishJobResponse,
    PublishPowJobRequest,
    PublishRewardJobRequest,
    QueryJobResponse,
    QueryMinedPointsResponse,
    QueryQueueLenResponse,
    QueryRewardJobsResponse,
    TakeJobResponse,
)
from mizu_node.db.job_queue import clear_jobs, get_assigned_reward_jobs, queue_clean

logging.basicConfig(level=logging.INFO)  # Set the desired logging level

# Security scheme
bearer_scheme = HTTPBearer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.conn = Connections()
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, queue_clean, app.state.conn)
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


REQUEST_TOTAL = Counter("app_http_request_count", "Total App HTTP Request")
REQUEST_TOTAL_WITH_LABEL = Counter(
    "app_http_request_count_with_label",
    "Total App HTTP Request With Labels",
    ["endpoint"],
)

OVERALL_LATENCY_WITH_LABEL = Histogram(
    "app_http_request_overall_latency_ms",
    "Overal Latency of App HTTP Request With Labels",
    ["endpoint"],
    buckets=LATENCY_BUCKETS,
)


@app.middleware("tracing")
def tracing(request: Request, call_next):
    REQUEST_TOTAL.inc()
    REQUEST_TOTAL_WITH_LABEL.labels(request.url.path).inc()
    start_time = epoch_ms()
    response = call_next(request)
    OVERALL_LATENCY_WITH_LABEL.labels(request.url.path).observe(epoch_ms() - start_time)
    return response


metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


def get_user(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> str:
    token = credentials.credentials
    return verify_jwt(token, os.environ["JWT_VERIFY_KEY"])


def get_caller(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> str:
    token = credentials.credentials
    with app.state.conn.get_pg_connection() as db:
        return verify_api_key(db, token)


@app.get("/")
@app.get("/healthcheck")
def default():
    return {"status": "ok"}


@app.post("/clear_queue")
@error_handler
def clear_queue(job_type: JobType, caller: str = Depends(get_caller)):
    validate_admin_job(caller)
    with app.state.conn.get_pg_connection() as db:
        clear_jobs(db, job_type)
    return build_ok_response()


@app.post("/publish_pow_jobs")
@error_handler
def publish_pow_jobs(
    request: PublishPowJobRequest,
    caller: str = Depends(get_caller),
):
    validate_admin_job(caller)
    with app.state.conn.get_pg_connection() as db:
        ids = handle_publish_jobs(db, caller, JobType.pow, request.data)
        return build_ok_response(PublishJobResponse(job_ids=ids))


@app.post("/publish_reward_jobs")
@error_handler
def publish_reward_jobs(
    request: PublishRewardJobRequest,
    caller: str = Depends(get_caller),
):
    validate_admin_job(caller)
    with app.state.conn.get_pg_connection() as db:
        ids = handle_publish_jobs(db, caller, JobType.reward, request.data)
        return build_ok_response(PublishJobResponse(job_ids=ids))


@app.post("/publish_batch_classify_jobs")
@error_handler
def publish_batch_classify_jobs(
    request: PublishBatchClassifyJobRequest, caller: str = Depends(get_caller)
):
    validate_admin_job(caller)
    with app.state.conn.get_pg_connection() as db:
        ids = handle_publish_jobs(db, caller, JobType.batch_classify, request.data)
        return build_ok_response(PublishJobResponse(job_ids=ids))


@app.get("/job_status")
@error_handler
def query_job_status(ids: List[str] = Query(None), _: str = Depends(get_caller)):
    with app.state.conn.get_pg_connection() as db:
        jobs = handle_query_job(db, ids)
        return build_ok_response(QueryJobResponse(jobs=jobs))


@app.get("/reward_jobs")
@error_handler
def query_reward_jobs(user: str = Depends(get_user)):
    with app.state.conn.get_pg_connection() as db:
        jobs = get_assigned_reward_jobs(db, user)
        return build_ok_response(QueryRewardJobsResponse(jobs=jobs))


TAKE_JOB = Counter("take_job", "# of take_job requests per job_type", ["job_type"])


@app.get("/take_job")
@error_handler
def take_job(
    job_type: JobType,
    user: str = Depends(get_user),
):
    job = handle_take_job(app.state.conn, user, job_type)
    TAKE_JOB.labels(job_type.name).inc()
    return build_ok_response(TakeJobResponse(job=job))


FINISH_JOB = Counter(
    "finish_job", "# of finish_job requests per job_type", ["job_type"]
)


@app.post("/finish_job")
@error_handler
def finish_job(request: FinishJobRequest, user: str = Depends(get_user)):
    points = handle_finish_job(app.state.conn, user, request.job_result)
    job_type = request.job_result.job_type
    FINISH_JOB.labels(job_type.name).inc()
    return build_ok_response(FinishJobResponse(rewarded_points=points))


@app.get("/reward_jobs_v2")
@error_handler
def query_reward_jobs(user: str, caller=Depends(get_caller)):
    validate_admin_job(caller)
    with app.state.conn.get_pg_connection() as db:
        jobs = get_assigned_reward_jobs(db, user)
        return build_ok_response(QueryRewardJobsResponse(jobs=jobs))


TAKE_JOB_V2 = Counter(
    "take_job_V2", "# of take_job requests per job_type", ["job_type"]
)


@app.get("/take_job_v2")
@error_handler
def take_job_v2(
    job_type: JobType,
    user: str,
    caller: str = Depends(get_caller),
):
    validate_admin_job(caller)
    job = handle_take_job(app.state.conn, user, job_type)
    TAKE_JOB_V2.labels(job_type.name).inc()
    return build_ok_response(TakeJobResponse(job=job))


FINISH_JOB_V2 = Counter(
    "finish_job_v2", "# of finish_job requests per job_type", ["job_type"]
)


@app.post("/finish_job_v2")
@error_handler
def finish_job_v2(request: FinishJobRequest, caller: str = Depends(get_caller)):
    validate_admin_job(caller)
    if request.user is None or request.user == "":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="user must be provided",
        )
    settle_reward = handle_finish_job_v2(
        app.state.conn, request.user, request.job_result
    )
    job_type = request.job_result.job_type
    FINISH_JOB_V2.labels(job_type.name).inc()
    return build_ok_response(FinishJobV2Response(settle_reward=settle_reward))


@app.get("/stats/queue_len")
@app.get("/global_stats/queue_len")
@error_handler
def queue_len(job_type: JobType = JobType.pow):
    """
    Return the number of queued classify jobs.
    """
    with app.state.conn.get_pg_connection() as db:
        q_len = handle_queue_len(db, job_type)
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
        points = total_mined_points_in_past_n_hour(app.state.conn.redis, max(hours, 24))
    if days is not None:
        points = total_mined_points_in_past_n_days(app.state.conn.redis, max(days, 7))
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
        points = total_rewarded_in_past_n_hour(
            app.state.conn.redis, token, max(hours, 24)
        )
    if days is not None:
        points = total_rewarded_in_past_n_days(
            app.state.conn.redis, token, max(days, 7)
        )
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
            app.state.conn.redis, user, max(hours, 24)
        )
    if days is not None:
        points = total_mined_points_in_past_n_days_per_worker(
            app.state.conn.redis, user, max(days, 7)
        )
    return build_ok_response(QueryMinedPointsResponse(points=points))


@app.get("/stats/mined_points_v2")
@app.get("/worker_stats/mined_points_v2")
@error_handler
def get_mined_points_v2(
    user: str,
    hours: int | None = None,
    days: int | None = None,
    caller=Depends(get_caller),
):
    """
    Return the mined points in the last `hours` hours or last `days` days.
    """
    validate_admin_job(caller)
    if hours is None and days is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="either hours or days must be provided",
        )
    if hours is not None:
        points = total_mined_points_in_past_n_hour_per_worker(
            app.state.conn.redis, user, max(hours, 24)
        )
    if days is not None:
        points = total_mined_points_in_past_n_days_per_worker(
            app.state.conn.redis, user, max(days, 7)
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
