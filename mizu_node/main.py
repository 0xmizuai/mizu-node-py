import uvicorn
from fastapi import FastAPI


from mizu_node.db.job import (
    ClassificationJobFromPublisher,
    ClassificationJobResultFromWorker,
    handle_take_job,
    handle_new_job,
    handle_finish_job,
    get_pending_jobs_num,
    get_processing_jobs_num,
)

app = FastAPI()


def get_caller() -> str:
    return ""


@app.get("/")
@app.get("/healthcheck")
async def default():
    return {"status": "ok"}


@app.get("stat/pending_jobs_len")
async def pending_jobs_len():
    return get_pending_jobs_num()


@app.get("stat/assigned_jobs_len")
async def assigned_jobs_len():
    return get_processing_jobs_num()


@app.get("take_job")
async def take_job():
    job_for_worker = handle_take_job(get_caller())
    return {"job": job_for_worker}


@app.post("add_job")
async def add_job(payload: ClassificationJobFromPublisher):
    handle_new_job(payload)
    return {"status": "ok"}


@app.post("finish_job")
async def finish_job(_id: str, tags: list[str]):
    handle_finish_job(ClassificationJobResultFromWorker(_id, get_caller(), tags))
    return {"status": "ok"}


# this job can only be called by validator
@app.post("verify_job")
async def verify_job():
    pass


def start_dev():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000, reload=True)


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000)
