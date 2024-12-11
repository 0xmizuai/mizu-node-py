import os

TEST_API_KEY1 = "test_api_key1"
TEST_API_KEY2 = "test_api_key2"


def pytest_configure():
    os.environ["POSTGRES_URL"] = (
        "postgresql://postgres:postgres@localhost:5432/postgres"
    )
    os.environ["REDIS_URL"] = "redis://localhost:6379"
    os.environ["MIZU_NODE_MONGO_DB_NAME"] = "mizu_node"
    os.environ["API_SECRET_KEY"] = "some-secret"
    os.environ["BACKEND_SERVICE_URL"] = "http://localhost:3000"
    os.environ["ACTIVE_USER_PAST_7D_THRESHOLD"] = "50"
    os.environ["MIN_REWARD_GAP"] = "1800"
    os.environ["ENABLE_ACTIVE_USER_CHECK"] = "true"
