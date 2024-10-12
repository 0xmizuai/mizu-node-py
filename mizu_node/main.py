import uvicorn
from fastapi import FastAPI


from mizu_node.handler import (
    ClassificationJob,
    ClassificationJobResult,
    handle_take_job,
    handle_add_job,
    handle_finish_job,
)

app = FastAPI()


@app.get("/")
@app.get("/healthcheck")
async def default(job: ClassificationJob):
    return {"status": "ok"}


@app.get("stat/queue_len")
async def queue_len():
    return handle_queue_len()


@app.get("stat/assigned_len")
async def queue_len():
    return handle_get_assigned_len()


@app.get("take_job")
async def take_job():
    return handle_take_job()


@app.post("add_job")
async def add_job(job: ClassificationJob):
    return handle_add_job(job)


@app.post("finish_job")
async def finish_job(result: ClassificationJobResult):
    return handle_finish_job(result)


def start_dev():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000, reload=True)


# the number of workers is defined by $WEB_CONCURRENCY env as default
def start():
    uvicorn.run("mizu_node.main:app", host="0.0.0.0", port=8000)
