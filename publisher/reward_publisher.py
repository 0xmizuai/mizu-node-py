import os
import random
import time
from typing import Optional

from pydantic import BaseModel, Field
from redis import Redis
import redis
from mizu_node.constants import REDIS_URL
from mizu_node.types.data_job import RewardContext, Token
from publisher.common import DataJobPublisher


class RewardJobConfig(BaseModel):
    key: str
    ctx: RewardContext
    budget_per_day: Optional[int] = None
    budget_per_week: Optional[int] = None
    batch_size: int = Field(default=1)


ARB_USDT = Token(
    chain="arbitrum",
    address="0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
    protocol="ERC20",
)


def usdt_ctx(amount: int):
    # decimal = 6
    return RewardContext(token=ARB_USDT, amount=amount * 1_000_000)


def point_ctx(amount: int):
    return RewardContext(token=None, amount=amount)


REWARD_CONFIGS = [
    RewardJobConfig(key="1usdt", ctx=usdt_ctx(1), budget_per_day=24),
    RewardJobConfig(key="5usdt", ctx=usdt_ctx(5), budget_per_day=1),
    RewardJobConfig(key="10usdt", ctx=usdt_ctx(10), budget_per_week=3),
    RewardJobConfig(key="100usdt", ctx=usdt_ctx(100), budget_per_week=1),
    RewardJobConfig(
        key="10points", ctx=point_ctx(10), budget_per_day=2000, batch_size=5
    ),
    RewardJobConfig(key="50points", ctx=point_ctx(50), budget_per_day=500),
    RewardJobConfig(key="100points", ctx=point_ctx(100), budget_per_day=100),
    RewardJobConfig(key="500points", ctx=point_ctx(500), budget_per_day=20),
]


class RewardJobPublisher(DataJobPublisher):
    def __init__(
        self,
        api_key: str,
        rclient: Redis,
        cron_gap: int = 60,  # run every 60 seconds
    ):
        super().__init__(api_key)
        self.rclient = rclient
        self.cron_gap = cron_gap
        self.num_of_runs_per_day = 86400 // self.cron_gap
        self.num_of_runs_per_week = 604800 // self.cron_gap

    def budget_per_day_key(self, key: str):
        day_n = int(time.time()) // 86400
        return f"{key}:budget_per_day:{day_n}"

    def budget_per_week_key(self, key: str):
        week_n = int(time.time()) // 604800
        return f"{key}:budget_per_week:{week_n}"

    def lottery(self, config: RewardJobConfig):
        if config.budget_per_day:
            key = self.budget_per_day_key(config.key)
            spent = self.rclient.get(key)
            if int(spent or 0) >= config.budget_per_day:
                return False

            max_release_per_day = config.budget_per_day / config.batch_size
            possibility = max_release_per_day / self.num_of_runs_per_day
            return random.uniform(0, 1) < possibility

        elif config.budget_per_week:
            key = self.budget_per_week_key(config.key)
            spent = self.rclient.get(key)
            if int(spent or 0) >= config.budget_per_week:
                return False

            max_release_per_week = config.budget_per_week / config.batch_size
            possibility = max_release_per_week / self.num_of_runs_per_week
            return random.uniform(0, 1) < possibility

        return False

    def endpoint(self):
        return "/publish_reward_jobs"

    def run(self):
        while True:
            contexts = [config.ctx for config in REWARD_CONFIGS if self.lottery(config)]
            self.publish(contexts)
            time.sleep(self.cron_gap)


def publish_reward_jobs():
    rclient = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    api_key = os.environ.get("MIZU_ADMIN_USER_API_KEY")
    RewardJobPublisher(api_key, rclient).run()
