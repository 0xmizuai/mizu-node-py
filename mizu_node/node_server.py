import asyncio
from contextlib import asynccontextmanager
import logging
import uvicorn
from fastapi import FastAPI, HTTPException, Request, status, Depends
from fastapi.middleware.cors import CORSMiddleware

from prometheus_client import Counter, Histogram, make_asgi_app

from mizu_node.common import build_ok_response, epoch_ms, error_handler
from mizu_node.constants import (
    LATENCY_BUCKETS,
)
from mizu_node.job_handler import (
    handle_finish_job_v2,
    handle_take_job,
)
from mizu_node.security import (
    get_allowed_origins,
    verify_internal_service,
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
from mizu_node.types.node_service import (
    FinishJobRequest,
    FinishJobV2Response,
    QueryMinedPointsResponse,
    QueryQueueLenResponse,
    QueryRewardJobsResponse,
    TakeJobResponse,
)
from mizu_node.db.job_queue import (
    get_assigned_reward_jobs,
    get_queue_len,
    queue_clean,
)

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.conn = Connections()
    asyncio.create_task(queue_clean(app.state.conn))
    yield
    await app.state.conn.close()


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

# Prometheus metrics setup
REQUEST_TOTAL = Counter("app_http_request_count", "Total App HTTP Request")
REQUEST_TOTAL_WITH_LABEL = Counter(
    "app_http_request_count_with_label",
    "Total App HTTP Request With Labels",
    ["endpoint"],
)

OVERALL_LATENCY_WITH_LABEL = Histogram(
    "app_http_request_overall_latency_ms",
    "Overall Latency of App HTTP Request With Labels",
    ["endpoint"],
    buckets=LATENCY_BUCKETS,
)


@app.middleware("http")
async def tracing(request: Request, call_next):
    REQUEST_TOTAL.inc()
    REQUEST_TOTAL_WITH_LABEL.labels(request.url.path).inc()
    start_time = epoch_ms()
    response = await call_next(request)
    OVERALL_LATENCY_WITH_LABEL.labels(request.url.path).observe(epoch_ms() - start_time)
    return response


metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/")
@app.get("/healthcheck")
async def default():
    return {"status": "ok"}


@app.get("/reward_jobs_v2")
@error_handler
async def query_reward_jobs(user: str, _: str = Depends(verify_internal_service)):
    async with app.state.conn.get_job_db_session() as session:
        jobs = await get_assigned_reward_jobs(session, user)
        return build_ok_response(QueryRewardJobsResponse(jobs=jobs))


TAKE_JOB_V2 = Counter(
    "take_job_V2", "# of take_job requests per job_type", ["job_type"]
)


@app.get("/take_job_v2")
@error_handler
async def take_job_v2(
    job_type: JobType,
    user: str,
    _: str = Depends(verify_internal_service),
):
    job = await handle_take_job(app.state.conn, user, job_type)
    TAKE_JOB_V2.labels(job_type.name).inc()

    logging.info(f"Job type: {type(job)}")
    response = build_ok_response(TakeJobResponse(job=job))
    logging.info(f"Response type: {type(response)}")
    return response


FINISH_JOB_V2 = Counter(
    "finish_job_v2", "# of finish_job requests per job_type", ["job_type"]
)


@app.post("/finish_job_v2")
@error_handler
async def finish_job_v2(
    request: FinishJobRequest, _: str = Depends(verify_internal_service)
):
    if request.user is None or request.user == "":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="user must be provided",
        )
    settle_reward = await handle_finish_job_v2(
        app.state.conn, request.user, request.job_result
    )
    job_type = request.job_result.job_type
    FINISH_JOB_V2.labels(job_type.name).inc()
    return build_ok_response(FinishJobV2Response(settle_reward=settle_reward))


@app.get("/stats/queue_len")
@app.get("/global_stats/queue_len")
@error_handler
async def queue_len(job_type: JobType = JobType.pow):
    """Return the number of queued classify jobs."""
    async with app.state.conn.get_job_db_session() as session:
        q_len = await get_queue_len(session, job_type)
        return build_ok_response(QueryQueueLenResponse(length=q_len))


@app.get("/global_stats/mined_points")
@error_handler
async def get_mined_points_stats(hours: int | None = None, days: int | None = None):
    """Return the mined points in the last `hours` hours or last `days` days."""
    if hours is None and days is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="either hours or days must be provided",
        )
    if hours is not None:
        points = await total_mined_points_in_past_n_hour(
            app.state.conn.redis, max(hours, 24)
        )
    if days is not None:
        points = await total_mined_points_in_past_n_days(
            app.state.conn.redis, max(days, 7)
        )
    return build_ok_response(QueryMinedPointsResponse(points=points))


@app.get("/global_stats/rewards")
@error_handler
async def get_rewards_stats(
    token: str, hours: int | None = None, days: int | None = None
):
    """Return the mined points in the last `hours` hours or last `days` days."""
    if hours is None and days is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="either hours or days must be provided",
        )
    if hours is not None:
        points = await total_rewarded_in_past_n_hour(
            app.state.conn.redis, token, max(hours, 24)
        )
    if days is not None:
        points = await total_rewarded_in_past_n_days(
            app.state.conn.redis, token, max(days, 7)
        )
    return build_ok_response(QueryMinedPointsResponse(points=points))


@app.get("/stats/mined_points_v2")
@app.get("/worker_stats/mined_points_v2")
@error_handler
async def get_mined_points_v2(
    user: str,
    hours: int | None = None,
    days: int | None = None,
    _: str = Depends(verify_internal_service),
):
    if hours is None and days is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="either hours or days must be provided",
        )
    if hours is not None:
        points = await total_mined_points_in_past_n_hour_per_worker(
            app.state.conn.redis, user, max(hours, 24)
        )
    if days is not None:
        points = await total_mined_points_in_past_n_days_per_worker(
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
        "mizu_node.node_server:app",
        host="0.0.0.0",
        port=8000,
        lifespan="on",
        reload=True,
    )
    apps.append(MyServer(config=config1).run())
    apps.append(MyServer(config=config2).run())
    return await asyncio.gather(*apps)


def start_dev():
    asyncio.run(run())


def start():
    uvicorn.run(
        "mizu_node.node_server:app", host=["::", "0.0.0.0"], lifespan="on", port=8000
    )
