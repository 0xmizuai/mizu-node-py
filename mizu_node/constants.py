import os

# redis for pending/assigned jobs
COOLDOWN_WORKER_EXPIRE_TTL_SECONDS = int(
    os.environ.get("COOLDOWN_WORKER_EXPIRE_TTL_SECONDS", 5)
)

R2_DATA_PREFIX = "https://rawdata.mizu.technology"

DEFAULT_POW_DIFFICULTY = int(os.environ.get("DEFAULT_POW_DIFFICULTY", 4))

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

LATENCY_BUCKETS = [
    1.0,
    5.0,
    10.0,
    50.0,
    100.0,
    500.0,
    1000.0,
    5000.0,
    10000.0,
    20000.0,
    30000.0,
    float("inf"),
]
