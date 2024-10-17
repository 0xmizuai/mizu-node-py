from redis import Redis
from rq import Queue
from mizu_node.constants import REDIS_URL, VERIFY_JOB_QUEUE_NAME

rclient = Redis.from_url(REDIS_URL)
verify_job_queue = Queue(VERIFY_JOB_QUEUE_NAME, connection=rclient)
