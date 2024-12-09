import argparse
import logging
import os
import random
import time

from pydantic import BaseModel
from redis import Redis
from mizu_node.common import epoch, is_prod
from mizu_node.types.data_job import RewardContext, Token
from mizu_node.types.service import PublishRewardJobRequest
from publisher.common import publish

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


class BudgetSetting(BaseModel):
    unit: int
    unit_name: str
    budget: int


class RewardJobConfig(BaseModel):
    key: str
    ctx: RewardContext
    budget: BudgetSetting | None = None


ARB_USDT = Token(
    chain="arbitrum",
    address="0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
    decimals=6,
    protocol="ERC20",
)

ARB_USDT_TEST = Token(
    chain="arbitrum_sepolia",
    address="0x0C5eAB07a5E082ED5Dc14BAC7e9C706568C2905f",
    decimals=18,
    protocol="ERC20",
)


def usdt_ctx(amount: int):
    # decimal = 6
    if is_prod():
        amount_str = str(int(amount * 10**ARB_USDT.decimals))
        return RewardContext(token=ARB_USDT, amount=amount_str)
    else:
        amount_str = str(int(amount * 10**ARB_USDT_TEST.decimals))
        return RewardContext(token=ARB_USDT_TEST, amount=amount_str)


def point_ctx(amount: float):
    return RewardContext(token=None, amount=str(amount))


def get_hourly_active_user():
    return int(os.environ.get("HOURLY_ACTIVE_USER", 100))


USDT_REWARD_CONFIGS = [
    RewardJobConfig(
        key="0.01usdt",
        ctx=usdt_ctx(0.01),
        budget=BudgetSetting(unit=3600, unit_name="hour", budget=120),  # 28.8u per day
    ),
    RewardJobConfig(
        key="0.02usdt",
        ctx=usdt_ctx(0.02),
        budget=BudgetSetting(unit=3600, unit_name="hour", budget=20),  # 9.6u per day
    ),
    RewardJobConfig(
        key="0.03usdt",
        ctx=usdt_ctx(0.03),
        budget=BudgetSetting(unit=3600, unit_name="hour", budget=10),  # 7.2u per day
    ),
]

POINTS_REWARD_CONFIGS = [
    RewardJobConfig(
        key="1point",
        ctx=point_ctx(1),
        budget=BudgetSetting(
            unit=3600, unit_name="hour", budget=get_hourly_active_user() * 3  # 3000
        ),  # 72k
    ),
    RewardJobConfig(
        key="2points",
        ctx=point_ctx(2),
        budget=BudgetSetting(
            unit=3600, unit_name="hour", budget=get_hourly_active_user()  # 1000
        ),  # 48k
    ),
    RewardJobConfig(
        key="3points",
        ctx=point_ctx(3),
        budget=BudgetSetting(
            unit=3600, unit_name="hour", budget=get_hourly_active_user() // 2  # 500
        ),  # 36k
    ),
    RewardJobConfig(
        key="5points",
        ctx=point_ctx(5),
        budget=BudgetSetting(unit=3600, unit_name="hour", budget=200),  # 24k
    ),
    RewardJobConfig(
        key="10points",
        ctx=point_ctx(10),
        budget=BudgetSetting(unit=3600, unit_name="hour", budget=120),  # 28k
    ),
    RewardJobConfig(
        key="50points",
        ctx=point_ctx(50),
        budget=BudgetSetting(unit=86400, unit_name="day", budget=100),  # 5k
    ),
    RewardJobConfig(
        key="100points",
        ctx=point_ctx(50),
        budget=BudgetSetting(unit=86400, unit_name="day", budget=50),  # 5k
    ),
    RewardJobConfig(
        key="500points",
        ctx=point_ctx(50),
        budget=BudgetSetting(unit=86400, unit_name="day", budget=10),  # 5k
    ),
]


def build_reward_configs(types: list[str]) -> list[RewardJobConfig]:
    REWARD_CONFIGS = []
    if "all" in types or "usdt" in types:
        REWARD_CONFIGS += USDT_REWARD_CONFIGS
    if "all" in types or "points" in types:
        REWARD_CONFIGS += POINTS_REWARD_CONFIGS
    return REWARD_CONFIGS


class RewardJobPublisher(object):
    def __init__(
        self,
        types: list[str],
        cron_gap: int = 60,  # run every 60 seconds
    ):
        self.reward_configs = build_reward_configs(types)
        self.api_key = os.environ["API_SECRET_KEY"]
        self.rclient = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        self.cron_gap = cron_gap

    def spent_key(self, config: RewardJobConfig):
        n = epoch() // config.budget.unit
        return f"{config.key}:spent_per_{config.budget.unit_name}:{n}"

    def spent(self, config: RewardJobConfig):
        spent = self.rclient.get(self.spent_key(config))
        return int(spent or 0)

    def record_spent(self, config: RewardJobConfig, amount: float):
        self.rclient.incrbyfloat(self.spent_key(config), amount)

    def lottery(self, config: RewardJobConfig):
        if self.spent(config) > config.budget.budget:
            return False
        total_runs = config.budget.unit // self.cron_gap
        return random.uniform(0, 1) < (config.budget.budget / total_runs)

    def get_batch_size(self, config: RewardJobConfig):
        total_runs = config.budget.unit // self.cron_gap
        if config.budget.budget > total_runs:
            return config.budget.budget // total_runs
        else:
            return 1

    def run(self):
        while True:
            contexts = []
            logging.info("======= start to publish reward jobs ======")
            for config in self.reward_configs:
                if self.lottery(config):
                    batch_size = self.get_batch_size(config)
                    logging.info(f"publishing {batch_size} reward jobs: {config.key}")
                    contexts.extend([config.ctx for _ in range(batch_size)])
                    self.record_spent(config, batch_size)
                else:
                    logging.info(f"no reward jobs for {config.key}")
            if len(contexts) > 0:
                publish(
                    "/publish_reward_jobs",
                    self.api_key,
                    PublishRewardJobRequest(data=contexts),
                )
                logging.info("all reward jobs published")
            else:
                logging.info("no reward job to publish")

            # print stats every 10 runs (10 minutes)
            if random.uniform(0, 1) < 0.1:
                self.print_stats()
            time.sleep(self.cron_gap)

    def print_stats(self):
        for config in self.reward_configs:
            logging.info(
                f">>>>>> {config.key} spent_per_{config.budget.unit_name}: {self.spent(config)}, budget_per_{config.budget.unit_name}: {config.budget.budget}"
            )


parser = argparse.ArgumentParser()
parser.add_argument(
    "--types", action="store", type=str, default="all", help="types to publish"
)
args = parser.parse_args()


def start():
    RewardJobPublisher(args.types.split(",")).run()
