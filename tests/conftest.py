import os


def pytest_configure():
    os.environ["POSTGRES_URL"] = (
        "postgresql://postgres:postgres@localhost:5432/postgres"
    )
    os.environ["REDIS_URL"] = "redis://localhost:6379"
    os.environ["API_SECRET_KEY"] = "some-secret"
    os.environ["ACTIVE_USER_PAST_7D_THRESHOLD"] = "50"
    os.environ["MIN_REWARD_GAP"] = "1800"
    os.environ["ENABLE_ACTIVE_USER_CHECK"] = "true"
