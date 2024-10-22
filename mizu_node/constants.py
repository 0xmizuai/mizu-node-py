import os

# redis for pending/assigned jobs
REDIS_JOB_QUEUE_NAME = os.environ.get("REDIS_JOB_QUEUE_NAME", "mizu_job_queue")
REDIS_URL = os.environ.get("REDIS_URL")
ASSIGNED_JOB_EXPIRE_TTL_SECONDS = int(
    os.environ.get("ASSIGNED_JOB_EXPIRE_TTL_SECONDS", 3600)
)
BLOCKED_WORKER_PREFIX = "blocked_worker:"
VERIFY_JOB_CALLBACK_URL = os.environ.get("VERIFY_JOB_CALLBACK_URL")
VERIFY_JOB_QUEUE_NAME = "verify_job_queue"
COOLDOWN_WORKER_PREFIX = "cooldown_worker:"
COOLDOWN_WORKER_EXPIRE_TTL_SECONDS = int(
    os.environ.get("COOLDOWN_WORKER_EXPIRE_TTL_SECONDS", 30)
)

# mongodb for finished jobs
MONGO_URL = os.environ.get("MONGO_URL")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME")

# misc
VERIFICATION_MODE = os.environ.get("VERIFICATION_MODE") or "random"
VERIFICATION_RATIO_BASE = 1000  # only valid when mode=random
