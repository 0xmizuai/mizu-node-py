import os

# redis for pending/assigned jobs
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
API_KEY_COLLECTION = "api_keys"
JOBS_COLLECTION = "jobs"
CLASSIFIER_COLLECTION = "classifiers"

R2_DATA_PREFIX = "https://rawdata.mizu.technology"

MIZU_NODE_MONGO_DB_NAME = os.environ.get("MIZU_NODE_MONGO_DB_NAME", "mizu_node")
