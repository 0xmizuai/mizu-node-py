import os

# redis for pending/processing jobs
REDIS_PENDING_JOBS_QUEUE = "queue:pending_jobs"
REDIS_PROCESSING_JOB_PREFIX = "processing_job:"
REDIS_TOTAL_PROCESSING_JOB = "total_processing_jobs"
REDIS_URL = os.environ["REDIS_URL"]
PROCESSING_JOB_EXPIRE_TTL_SECONDS = 3600

SHADOW_KEY_PREFIX = "shadow_key:"

# mongodb for finished jobs
FINISH_JOB_CALLBACK_URL = os.environ["FINISH_JOB_CALLBACK_URL"]
MONGO_URL = os.environ["MONGO_URL"]
MONGO_DB_NAME = os.environ["MONGO_DB_NAME"]
