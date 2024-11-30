from fastapi import FastAPI

app = FastAPI()


@app.get("/")
@app.get("/healthcheck")
def default():
    return {"status": "ok"}


@app.post("/api/settle_reward")
def settle_reward():
    return {"status": "ok"}
