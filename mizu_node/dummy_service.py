from fastapi import FastAPI
import uvicorn

app = FastAPI()


@app.get("/")
@app.get("/healthcheck")
def default():
    return {"status": "ok"}


@app.post("/settle_rewards")
def settle_rewards():
    return {"status": "ok"}
