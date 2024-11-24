import math
import secrets
import time
import requests
from mizu_node.security import MIZU_ADMIN_USER_API_KEY
from mizu_node.types.data_job import PowContext
from mizu_node.types.service import PublishPowJobRequest
from publisher.common import NODE_SERVICE_URL, publish


class PowDataJobPublisher(object):

    def __init__(
        self,
        batch_size: int = 1000,
        cooldown: int = 300,  # check every 5 mins
        threshold: int = 500_000,  # auto-publish when queue length is below 1_000_000
    ):
        self.api_key = MIZU_ADMIN_USER_API_KEY
        self.batch_size = batch_size
        self.threshold = threshold
        self.cooldown = cooldown

    def check_queue_stats(self):
        result = requests.get(
            f"{NODE_SERVICE_URL}/stats/queue_len?job_type=0",
        )
        length = result.json()["data"]["length"]
        if length > self.threshold:
            return 0
        return math.ceil(self.threshold * 2 - length)

    def run(self):
        while True:
            num_of_jobs = self.check_queue_stats()
            num_of_batches = math.ceil(num_of_jobs / self.batch_size)
            for _ in range(num_of_batches):
                contexts = [
                    PowContext(difficulty=4, seed=secrets.token_hex(32))
                    for _ in range(self.batch_size)
                ]
                print(f"Publishing {len(contexts)} pow jobs")
                publish(
                    "/publish_pow_jobs",
                    self.api_key,
                    PublishPowJobRequest(data=contexts),
                )
            time.sleep(self.cooldown)


def start():
    PowDataJobPublisher().run()
