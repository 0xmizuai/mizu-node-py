import os

# redis for pending/assigned jobs
REDIS_JOB_QUEUE_NAME = os.environ.get("REDIS_JOB_QUEUE_NAME", "mizu_job_queue")
REDIS_URL = os.environ.get("REDIS_URL")
ASSIGNED_JOB_EXPIRE_TTL_SECONDS = int(
    os.environ.get("ASSIGNED_JOB_EXPIRE_TTL_SECONDS", 3600)
)
BLOCKED_WORKER_PREFIX = "blocked_worker:"
COOLDOWN_WORKER_PREFIX = "cooldown_worker:"
COOLDOWN_WORKER_EXPIRE_TTL_SECONDS = int(
    os.environ.get("COOLDOWN_WORKER_EXPIRE_TTL_SECONDS", 30)
)

# mongodb for finished jobs
MONGO_URL = os.environ["MONGO_URL"]
MONGO_DB_NAME = os.environ["MONGO_DB_NAME"]
API_KEY_COLLECTION = "api_keys"
FINISHED_JOBS_COLLECTIONS = "finished_jobs"

VERIFY_KEY = os.environ["VERIFY_KEY"]
