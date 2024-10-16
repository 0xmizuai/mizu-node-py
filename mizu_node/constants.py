import os

# redis for pending/assigned jobs
REDIS_PENDING_JOBS_QUEUE = "queue:pending_jobs"
REDIS_ASSIGNED_JOB_PREFIX = "assigned_job:"
REDIS_TOTAL_ASSIGNED_JOB = "total_assigned_jobs"
REDIS_URL = os.environ.get("REDIS_URL")
ASSIGNED_JOB_EXPIRE_TTL_SECONDS = 3600

SHADOW_KEY_PREFIX = "shadow_key:"
BLOCKED_WORKER_PREFIX = "blocked_worker:"

# mongodb for finished jobs
FINISH_JOB_CALLBACK_URL = os.environ.get("FINISH_JOB_CALLBACK_URL")
VERIFY_JOB_URL = os.environ.get("VERIFY_JOB_URL")
VERIFY_JOB_CALLBACK_URL = os.environ.get("VERIFY_JOB_CALLBACK_URL")
MONGO_URL = os.environ.get("MONGO_URL")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME")

# misc
VERIFICATION_RATIO_BASE = 1000
