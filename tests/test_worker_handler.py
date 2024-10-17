from tests.redis_mock import RedisMock

from mizu_node.worker_handler import has_worker_cooled_down


def test_has_worker_cooled_down():
    r_client = RedisMock()
    user = "some_user"
    assert has_worker_cooled_down(r_client, user) is True
    assert has_worker_cooled_down(r_client, user) is False
