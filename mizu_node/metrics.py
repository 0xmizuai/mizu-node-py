from contextlib import asynccontextmanager
import logging
import uvicorn
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware

from mizu_node.common import build_ok_response, error_handler
from mizu_node.config import (
    get_allowed_origins,
)
from mizu_node.queue_watcher import watch
from mizu_node.stats import (
    total_mined_points_in_past_n_days,
    total_mined_points_in_past_n_hour,
    total_rewarded_in_past_n_days,
    total_rewarded_in_past_n_hour,
)
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import JobType
from mizu_node.types.service import (
    QueryMinedPointsResponse,
    QueryQueueLenResponse,
)
from mizu_node.db.job_queue import get_queue_len

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = Connections()
    app.state.conn = conn
    watch(conn)
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


def start_dev():
    uvicorn.run(
        "mizu_node.metrics:app", host="0.0.0.0", port=8001, lifespan="on", reload=True
    )


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run(
        "mizu_node.metrics:app", host=["::", "0.0.0.0"], lifespan="on", port=8001
    )
