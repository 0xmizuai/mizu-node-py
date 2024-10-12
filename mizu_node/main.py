import uvicorn
from fastapi import FastAPI


from mizu_node.job_handler import (
    ClassificationJob,
    ClassificationJobResult,
    handle_assigned_jobs_len,
    handle_pending_jobs_len,
    handle_take_job,
    handle_add_job,
    handle_finish_job,
)

app = FastAPI()


def get_caller():
    pass


@app.get("/")
@app.get("/healthcheck")
async def default(job: ClassificationJob):
    return {"status": "ok"}


@app.get("stat/pending_jobs_len")
async def pending_jobs_len():
    return handle_pending_jobs_len()


@app.get("stat/assigned_jobs_len")
async def assigned_jobs_len():
    return handle_assigned_jobs_len()


@app.get("take_job")
async def take_job():
    return handle_take_job(get_caller())


@app.post("add_job")
async def add_job(payload: ClassificationJob):
    return handle_add_job(payload)


@app.post("finish_job")
async def finish_job(payload: ClassificationJobResult):
    return handle_finish_job(get_caller(), payload)


@app.post("verify_job")
async def verify_job():
    pass


def start_dev():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000, reload=True)


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000)
