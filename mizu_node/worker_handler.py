import json
from redis import Redis

from mizu_node.constants import (
    COOLDOWN_WORKER_PREFIX,
    COOLDOWN_WORKER_EXPIRE_TTL_SECONDS,
)
from mizu_node.utils import epoch


def has_worker_cooled_down(r_client: Redis, worker: str) -> bool:
    if r_client.exists(COOLDOWN_WORKER_PREFIX + worker):
        return False
    r_client.setex(
        COOLDOWN_WORKER_PREFIX + worker,
        COOLDOWN_WORKER_EXPIRE_TTL_SECONDS,
        json.dumps({"cool_down": True, "last_received": epoch()}),
    )
    return True
