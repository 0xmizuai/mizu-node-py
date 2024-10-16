import redis

from mizu_node.constants import (
    REDIS_ASSIGNED_JOB_PREFIX,
    SHADOW_KEY_PREFIX,
)
from mizu_node.types import PendingJob, AssignedJob
from mizu_node.job_handler import (
    _add_new_jobs,
    _remove_assigned_job,
)


# Whenever key expire notification comes this function get's executed
def event_handler(rclient: redis.Redis, msg):
    try:
        key = msg["data"].decode("utf-8")
        # If shadowKey is there then it means we need to proceed or else just ignore it
        if SHADOW_KEY_PREFIX in key:
            # To get original key we are removing the shadowKey prefix
            key = key.replace(SHADOW_KEY_PREFIX, "")
            value = rclient.get(key)
            if REDIS_ASSIGNED_JOB_PREFIX in key:
                assigned = AssignedJob.model_validate_json(value)
                _add_new_jobs(rclient, [assigned])
                _remove_assigned_job(rclient, assigned.job_id)
            # Once we got to know the value we remove it from Redis and do whatever required
            rclient.delete(key)
    except Exception as exp:
        pass
