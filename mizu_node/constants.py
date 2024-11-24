import os

# redis for pending/assigned jobs
REDIS_URL = os.environ.get("REDIS_URL")
ASSIGNED_JOB_EXPIRE_TTL_SECONDS = int(
    os.environ.get("ASSIGNED_JOB_EXPIRE_TTL_SECONDS", 600)
)
BLOCKED_WORKER_PREFIX = "blocked_worker"
COOLDOWN_WORKER_PREFIX = "cooldown_worker"
COOLDOWN_WORKER_EXPIRE_TTL_SECONDS = int(
    os.environ.get("COOLDOWN_WORKER_EXPIRE_TTL_SECONDS", 5)
)

# mongodb for finished jobs
API_KEY_COLLECTION = "api_keys"
JOBS_COLLECTION = "jobs"
CLASSIFIER_COLLECTION = "classifiers"

R2_DATA_PREFIX = "https://rawdata.mizu.technology"

MIZU_NODE_MONGO_DB_NAME = os.environ.get("MIZU_NODE_MONGO_DB_NAME", "mizu_node")
DEFAULT_POW_DIFFICULTY = 5

MIZU_ADMIN_USER = "mizu.admin"
ACTIVE_USER_PAST_24H_THRESHOLD = int(
    os.environ.get("ACTIVE_USER_PAST_24H_THRESHOLD", 200)
)
ACTIVE_USER_PAST_7D_THRESHOLD = int(
    os.environ.get("ACTIVE_USER_PAST_7D_THRESHOLD", 100)
)
MIN_REWARD_GAP = int(os.environ.get("MIN_REWARD_GAP", 1800))

MAX_RETRY_ALLOWED = int(os.environ.get("MAX_RETRY_ALLOWED", 3))
