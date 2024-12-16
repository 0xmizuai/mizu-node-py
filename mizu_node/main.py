from contextlib import asynccontextmanager
import logging
import os
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, Security, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware

from prometheus_client import Counter, Histogram, make_asgi_app

from mizu_node.common import build_ok_response, epoch_ms, error_handler
from mizu_node.config import (
    LATENCY_BUCKETS,
)
from mizu_node.job_handler import (
    handle_finish_job_v2,
    handle_take_job,
)
from mizu_node.config import (
    get_allowed_origins,
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
from mizu_node.types.data_job import DataJobContext, JobType, RewardContext
from mizu_node.types.service import (
    FinishJobRequest,
    FinishJobV2Response,
    PublishRewardJobsRequest,
    PublishRewardJobsResponse,
    QueryMinedPointsResponse,
    QueryQueueLenResponse,
    QueryRewardJobsResponse,
    TakeJobResponse,
)
from mizu_node.db.job_queue import (
    add_jobs,
    get_assigned_reward_jobs,
    get_queue_len,
)

logging.basicConfig(level=logging.INFO)  # Set the desired logging level

# Security scheme
bearer_scheme = HTTPBearer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = Connections()
    app.state.conn = conn
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


def verify_internal_service(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> str:
    if credentials.credentials != os.environ["API_SECRET_KEY"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    return True


@app.get("/")
@app.get("/healthcheck")
def default():
    return {"status": "ok"}


@app.post("/publish_reward_jobs")
@error_handler
def publish_reward_jobs(
    request: PublishRewardJobsRequest, _=Depends(verify_internal_service)
):
    with app.state.conn.get_pg_connection() as db:
        contexts = [
            DataJobContext(reward_ctx=RewardContext(**job)) for job in request.jobs
        ]
        job_ids = add_jobs(db, JobType.reward, contexts, request.reference_id)
    return build_ok_response(PublishRewardJobsResponse(job_ids=job_ids))


@app.get("/reward_jobs_v2")
@error_handler
def query_reward_jobs(user: str, _=Depends(verify_internal_service)):
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
    reference_ids: str = Query(default="0"),
    _: str = Depends(verify_internal_service),
):
    TAKE_JOB_V2.labels(job_type.name).inc()
    reference_ids = reference_ids.split(",")
    for reference_id in reference_ids:
        job = handle_take_job(app.state.conn, user, job_type, int(reference_id))
        if job is not None:
            return build_ok_response(TakeJobResponse(job=job))
    return build_ok_response(TakeJobResponse(job=None))


FINISH_JOB_V2 = Counter(
    "finish_job_v2", "# of finish_job requests per job_type", ["job_type"]
)


@app.post("/finish_job_v2")
@error_handler
def finish_job_v2(request: FinishJobRequest, _: str = Depends(verify_internal_service)):
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
def queue_len(job_type: JobType = JobType.pow, reference_ids: list[int] = Query([])):
    """
    Return the number of queued classify jobs.
    """
    with app.state.conn.get_pg_connection() as db:
        q_len = get_queue_len(db, job_type, reference_ids)
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


@app.get("/stats/mined_points_v2")
@app.get("/worker_stats/mined_points_v2")
@error_handler
def get_mined_points_v2(
    user: str,
    hours: int | None = None,
    days: int | None = None,
    _=Depends(verify_internal_service),
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


def start_dev():
    uvicorn.run(
        "mizu_node.main:app", host="0.0.0.0", port=8000, lifespan="on", reload=True
    )


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run("mizu_node.main:app", host=["::", "0.0.0.0"], lifespan="on", port=8000)
