import logging
import os
import random
import time
from typing import Optional

from pydantic import BaseModel, Field
from redis import Redis
from mizu_node.types.data_job import RewardContext, Token
from mizu_node.types.service import PublishRewardJobRequest
from publisher.common import publish

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


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


class RewardJobPublisher(object):
    def __init__(
        self,
        cron_gap: int = 60,  # run every 60 seconds
    ):
        self.api_key = os.environ["MIZU_ADMIN_USER_API_KEY"]
        self.rclient = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        self.cron_gap = cron_gap
        self.num_of_runs_per_day = 86400 // self.cron_gap
        self.num_of_runs_per_week = 604800 // self.cron_gap

    def spent_per_day_key(self, key: str):
        day_n = int(time.time()) // 86400
        return f"{key}:spent_per_day:{day_n}"

    def spent_per_day(self, key: str):
        spent = self.rclient.get(self.spent_per_day_key(key))
        return int(spent or 0)

    def spent_per_week_key(self, key: str):
        week_n = int(time.time()) // 604800
        return f"{key}:spent_per_week:{week_n}"

    def spent_per_week(self, key: str):
        spent = self.rclient.get(key)
        return int(spent or 0)

    def record_spent_per_day(self, config: RewardJobConfig):
        self.rclient.incr(self.spent_per_day_key(config.key), config.batch_size)

    def record_spent_per_week(self, config: RewardJobConfig):
        self.rclient.incr(self.spent_per_week_key(config.key), config.batch_size)

    def lottery(self, config: RewardJobConfig):
        if config.budget_per_day:
            spent = self.spent_per_day(config.key)
            if spent >= config.budget_per_day:
                return False

            max_release_per_day = config.budget_per_day / config.batch_size
            possibility = max_release_per_day / self.num_of_runs_per_day
            if random.uniform(0, 1) < possibility:
                self.record_spent_per_day(config)
                return True

        elif config.budget_per_week:
            spent = self.spent_per_week(config.key)
            if spent >= config.budget_per_week:
                return False

            max_release_per_week = config.budget_per_week / config.batch_size
            possibility = max_release_per_week / self.num_of_runs_per_week
            if random.uniform(0, 1) < possibility:
                self.record_spent_per_week(config)
                return True

        return False

    def print_stats(self):
        for config in REWARD_CONFIGS:
            if config.budget_per_day:
                logging.info(
                    f"{config.key} spent: {self.spent_per_day(config.key)}, budget: {config.budget_per_day}"
                )
            elif config.budget_per_week:
                logging.info(
                    f"{config.key} spent: {self.spent_per_week(config.key)}, budget: {config.budget_per_week}"
                )

    def run(self):
        while True:
            configs = [config for config in REWARD_CONFIGS if self.lottery(config)]
            if len(configs) > 0:
                logging.info(
                    f"publishing {len(configs)} reward jobs: {[config.key for config in configs]}"
                )
                request = PublishRewardJobRequest(
                    data=[
                        config.ctx
                        for config in configs
                        for _ in range(config.batch_size)
                    ]
                )
                publish("/publish_reward_jobs", self.api_key, request)
            else:
                logging.info("no reward job to publish")

            # print stats every 10 runs (10 minutes)
            if random.uniform(0, 1) < 0.1:
                self.print_stats()

            time.sleep(self.cron_gap)


def start():
    RewardJobPublisher().run()
