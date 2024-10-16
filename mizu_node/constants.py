import os

# redis for pending/assigned jobs
REDIS_PENDING_JOBS_QUEUE = "queue:pending_jobs"
REDIS_ASSIGNED_JOB_PREFIX = "assigned_job:"
REDIS_TOTAL_ASSIGNED_JOB = "total_assigned_jobs"
REDIS_URL = os.environ["REDIS_URL"]
ASSIGNED_JOB_EXPIRE_TTL_SECONDS = 3600

SHADOW_KEY_PREFIX = "shadow_key:"
BLOCKED_WORKER_PREFIX = "blocked_worker:"

# mongodb for finished jobs
FINISH_JOB_CALLBACK_URL = os.environ["FINISH_JOB_CALLBACK_URL"]
VERIFY_JOB_URL = os.environ["VERIFY_JOB_URL"]
VERIFY_JOB_CALLBACK_URL = os.environ["VERIFY_JOB_CALLBACK_URL"]
MONGO_URL = os.environ["MONGO_URL"]
MONGO_DB_NAME = os.environ["MONGO_DB_NAME"]

# r2 for raw data
ACCOUTN_ID = os.environ["R2_ACCOUNT_ID"]
R2_ENDPOINT = f"https://{ACCOUTN_ID}.r2.cloudflarestorage.com"
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]

# misc
VERIFICATION_RATIO_BASE = 1000
