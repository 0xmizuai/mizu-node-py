from fastapi import FastAPI

app = FastAPI()


@app.get("/")
@app.get("/healthcheck")
def default():
    return {"status": "ok"}


@app.post("/settle_rewards")
def settle_rewards():
    return {"status": "ok"}
