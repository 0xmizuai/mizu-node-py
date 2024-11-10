from mizu_node.security import has_worker_cooled_down
from tests.redis_mock import RedisMock


def test_has_worker_cooled_down():
    r_client = RedisMock()
    user = "some_user"
    assert has_worker_cooled_down(r_client, user) is True
    assert has_worker_cooled_down(r_client, user) is False
