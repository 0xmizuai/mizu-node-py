import os
from mizu_node.types.data_job import JobType, Token
from mizu_node.types.service import CooldownConfig


ACTIVE_USER_PAST_7D_THRESHOLD = int(os.environ.get("ACTIVE_USER_PAST_7D_THRESHOLD", 50))
MIN_REWARD_GAP = int(os.environ.get("MIN_REWARD_GAP", 1800))
REWARD_TTL = int(os.environ.get("REWARD_TTL", 43200 + 300))

DEFAULT_POW_DIFFICULTY = int(os.environ.get("DEFAULT_POW_DIFFICULTY", 4))
MAX_RETRY_ALLOWED = int(os.environ.get("MAX_RETRY_ALLOWED", 3))

MAX_UNCLAIMED_REWARD = 5

LATENCY_BUCKETS = [
    1.0,
    5.0,
    10.0,
    50.0,
    100.0,
    500.0,
    1000.0,
    5000.0,
    10000.0,
    20000.0,
    30000.0,
    float("inf"),
]

USDT_ADDRESS = "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"
USDT_TEST_ADDRESS = "0x0C5eAB07a5E082ED5Dc14BAC7e9C706568C2905f"

ARB_USDT = Token(
    chain="arbitrum",
    address=USDT_ADDRESS,
    decimals=6,
    protocol="ERC20",
)

ARB_USDT_TEST = Token(
    chain="arbitrum_sepolia",
    address=USDT_TEST_ADDRESS,
    decimals=18,
    protocol="ERC20",
)


def get_cooldown_config(job_type: JobType) -> CooldownConfig:
    if job_type == JobType.reward:
        return CooldownConfig(60, 1)
    return CooldownConfig(5, 10)


def get_allowed_origins() -> list[str]:
    return os.environ.get("ALLOWED_ORIGINS", "*").split(",")


def get_lease_ttl(job_type: JobType) -> int:
    if job_type == JobType.reward:
        return 3600
    elif job_type == JobType.batch_classify:
        return 1200
    else:
        return 600


def get_min_queue_len(job_type: JobType) -> int:
    if job_type == JobType.pow:
        return 200000
    else:
        return 50000


def is_usdt(chain: str, address: str):
    return chain == "arbitrum" and address == USDT_ADDRESS


def is_usdt_test(chain: str, address: str):
    return chain == "arbitrum_sepolia" and address == USDT_TEST_ADDRESS
