import argparse
import logging
import math
import os
import secrets
import time
import requests
from mizu_node.types.data_job import PowContext
from mizu_node.types.service import PublishPowJobRequest
from publisher.common import NODE_SERVICE_URL, publish

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


class PowDataJobPublisher(object):

    def __init__(
        self,
        batch_size: int,
        cooldown: int,
        threshold: int,
    ):
        self.api_key = os.environ["MIZU_ADMIN_USER_API_KEY"]
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
            logging.info(f"will publish {num_of_jobs} pow jobs")
            num_of_batches = math.ceil(num_of_jobs / self.batch_size)
            for batch in range(num_of_batches):
                contexts = [
                    PowContext(difficulty=4, seed=secrets.token_hex(32))
                    for _ in range(self.batch_size)
                ]
                logging.info(
                    f"Publishing {self.batch_size} pow jobs: batch {batch} out of {num_of_batches}"
                )
                publish(
                    "/publish_pow_jobs",
                    self.api_key,
                    PublishPowJobRequest(data=contexts),
                )
            logging.info(f"all pow jobs published")
            time.sleep(self.cooldown)


parser = argparse.ArgumentParser()
parser.add_argument("--batch_size", type=int, action="store", default="1000")
parser.add_argument("--cooldown", type=int, action="store", default="300")
parser.add_argument("--threshold", type=int, action="store", default="500000")
args = parser.parse_args()


def start():
    PowDataJobPublisher(
        batch_size=args.batch_size, cooldown=args.cooldown, threshold=args.threshold
    ).run()
