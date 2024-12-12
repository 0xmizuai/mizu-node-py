from fastapi import FastAPI

app = FastAPI()


@app.get("/")
@app.get("/healthcheck")
async def default():
    return {"status": "ok"}


@app.post("/api/settle_reward")
async def settle_reward():
    return {"status": "ok"}
