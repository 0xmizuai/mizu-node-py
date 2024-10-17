import os

# redis for pending/assigned jobs
REDIS_PENDING_JOBS_QUEUE = "queue:pending_jobs"
REDIS_ASSIGNED_JOB_PREFIX = "assigned_job:"
REDIS_TOTAL_ASSIGNED_JOBS = "total_assigned_jobs"
REDIS_URL = os.environ.get("REDIS_URL")
ASSIGNED_JOB_EXPIRE_TTL_SECONDS = (
    os.environ.get("ASSIGNED_JOB_EXPIRE_TTL_SECONDS") or 3600
)

SHADOW_KEY_PREFIX = "shadow_key:"
BLOCKED_WORKER_PREFIX = "blocked_worker:"
VERIFY_JOB_QUEUE_NAME = "verify_job_queue"

# mongodb for finished jobs
MONGO_URL = os.environ.get("MONGO_URL")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME")

# misc
VERIFICATION_MODE = os.environ.get("VERIFICATION_MODE") or "random"
VERIFICATION_RATIO_BASE = 1000  # only valid when mode=random
