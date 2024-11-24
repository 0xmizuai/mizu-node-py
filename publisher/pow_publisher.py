import math
import secrets
import time
from typing import Iterator
import requests
from mizu_node.types.data_job import PowContext
from mizu_node.types.service import PublishPowJobRequest
from publisher.common import DataJobPublisher, get_api_key


class PowDataJobPublisher(DataJobPublisher):

    def __init__(
        self,
        api_key: str,
        num_of_threads: int,
        batch_size: int = 1000,
        cooldown: int = 300,  # check every 5 mins
        threshold: int = 1_000_000,  # auto-publish when queue length is below 1_000_000
    ):
        super().__init__(api_key)
        self.batch_size = batch_size
        self.threshold = threshold
        self.num_of_threads = num_of_threads
        self.cooldown = cooldown

    def _build_pow_ctx(self):
        return (PowContext(difficulty=5, seed=secrets.token_hex(32)),)

    def check_queue_stats(self):
        result = requests.post(
            self.service_url + "/stats/queue_len?job_type=0",
        )
        length = result.json()["data"]["length"]
        if length > self.threshold:
            return
        return math.ceil((self.threshold - length) / self.num_of_threads)

    def publish_in_batches(self, contexts: list[PowContext]) -> Iterator[str]:
        total = 0
        while total < len(contexts):
            if total < self.batch_size:
                self.publish(PublishPowJobRequest(data=contexts[total:-1]))
                return
            else:
                data = contexts[total : total + self.batch_size]
                self.publish(PublishPowJobRequest(data=data))
                total += self.batch_size

    def endpoint(self):
        return "/publish_pow_jobs"

    def run(self):
        while True:
            num_of_jobs = self.check_queue_stats()
            contexts = [self._build_pow_ctx() for _ in range(num_of_jobs)]
            self.publish_in_batches(contexts)
            time.sleep(self.cool_down)


def publish_pow_jobs(user: str, num_of_threads: int = 1):
    api_key = get_api_key(user)
    threads = []
    for _ in range(num_of_threads):
        threads.append(PowDataJobPublisher(api_key, num_of_threads))
        threads[-1].start()
    for t in threads:
        t.join()
