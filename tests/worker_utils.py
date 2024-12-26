from mizu_node.common import epoch
from mizu_node.stats import (
    event_name,
    rate_limit_field,
)
from mizu_node.types.data_job import JobType
from tests.redis_mock import RedisMock


def set_cooldown(rclient: RedisMock, worker: str, job_type: JobType):
    rclient.hset(event_name(worker), rate_limit_field(job_type), str(epoch()))


def clear_cooldown(rclient: RedisMock, worker: str, job_type: JobType):
    rclient.hset(event_name(worker), rate_limit_field(job_type), "0")
