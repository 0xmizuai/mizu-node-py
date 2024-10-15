import redis

from mizu_node.constants import (
    REDIS_PENDING_JOBS_QUEUE,
    REDIS_PROCESSING_JOB_PREFIX,
    REDIS_URL,
    SHADOW_KEY_PREFIX,
)
from mizu_node.types import ClassificationJobFromPublisher, ProcessingJob
from mizu_node.job_handler import (
    _add_new_jobs,
    _remove_processing_job,
)


def retry_expired_job(job: ProcessingJob):
    job = ClassificationJobFromPublisher(
        _id=job._id,
        publisher=job.publisher,
        created_at=job.created_at,
    )
    _add_new_jobs(rclient, [job])
    _remove_processing_job(rclient, job._id)


# Whenever key expire notification comes this function get's executed
def event_handler(msg):
    try:
        key = msg["data"].decode("utf-8")
        # If shadowKey is there then it means we need to proceed or else just ignore it
        if SHADOW_KEY_PREFIX in key:
            # To get original key we are removing the shadowKey prefix
            key = key.replace(SHADOW_KEY_PREFIX, "")
            value = rclient.get(key)
            if REDIS_PROCESSING_JOB_PREFIX in key:
                retry_expired_job(ProcessingJob.model_validate_json(value))
            # Once we got to know the value we remove it from Redis and do whatever required
            rclient.delete(key)
    except Exception as exp:
        pass


# Creating Redis and pubsub Connection
rclient = redis.Redis(REDIS_URL)
pubsub = rclient.pubsub()

# Set config in config file "notify-keyspace-events Ex"
# Subscribing to key expire events and whenver we get any notification sending it to event_handler function
pubsub.psubscribe(**{"__keyevent@0__:expired": event_handler})
pubsub.run_in_thread(sleep_time=0.01)
