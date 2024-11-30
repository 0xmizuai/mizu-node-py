import os

# redis for pending/assigned jobs
REDIS_URL = os.environ.get("REDIS_URL")
COOLDOWN_WORKER_EXPIRE_TTL_SECONDS = int(
    os.environ.get("COOLDOWN_WORKER_EXPIRE_TTL_SECONDS", 5)
)

# mongodb for finished jobs
API_KEY_COLLECTION = "api_keys"
JOBS_COLLECTION = "jobs"
CLASSIFIER_COLLECTION = "classifiers"

R2_DATA_PREFIX = "https://rawdata.mizu.technology"

MIZU_NODE_MONGO_DB_NAME = os.environ.get("MIZU_NODE_MONGO_DB_NAME", "mizu_node")
DEFAULT_POW_DIFFICULTY = int(os.environ.get("DEFAULT_POW_DIFFICULTY", 4))

MIZU_ADMIN_USER = "mizu.admin"
ACTIVE_USER_PAST_24H_THRESHOLD = int(
    os.environ.get("ACTIVE_USER_PAST_24H_THRESHOLD", 50)
)
ACTIVE_USER_PAST_7D_THRESHOLD = int(os.environ.get("ACTIVE_USER_PAST_7D_THRESHOLD", 50))
MIN_REWARD_GAP = int(os.environ.get("MIN_REWARD_GAP", 1800))
REWARD_TTL = int(
    os.environ.get("REWARD_TTL", 43200 + 300)
)  # 12 hours + 5 minutes buffer

MAX_RETRY_ALLOWED = int(os.environ.get("MAX_RETRY_ALLOWED", 3))

MAX_UNCLAIMED_REWARD = 5
