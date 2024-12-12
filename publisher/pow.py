import argparse
import logging
import math
import os
import secrets
from mizu_node.db.job_queue import queue_len, add_jobs
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import DataJobContext, JobType, PowContext
import asyncio

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


class PowDataJobPublisher(object):

    def __init__(
        self,
        batch_size: int,
        cooldown: int,
        threshold: int,
    ):
        self.api_key = os.environ["API_SECRET_KEY"]
        self.batch_size = batch_size
        self.threshold = threshold
        self.cooldown = cooldown
        self.conn = Connections()

    async def check_queue_stats(self) -> int:
        async with self.conn.get_job_db_session() as db:
            current_len = await queue_len(db, JobType.pow)
            if current_len >= self.threshold:
                return 0
            return math.ceil(self.threshold * 2 - current_len)

    async def run(self):
        while True:
            num_of_jobs = await self.check_queue_stats()
            logging.info(f"will publish {num_of_jobs} pow jobs")

            if num_of_jobs > 0:
                num_of_batches = math.ceil(num_of_jobs / self.batch_size)
                for batch in range(num_of_batches):
                    contexts = [
                        DataJobContext(
                            pow_ctx=PowContext(difficulty=4, seed=secrets.token_hex(32))
                        )
                        for _ in range(self.batch_size)
                    ]
                    logging.info(
                        f"Publishing {self.batch_size} pow jobs: batch {batch} out of {num_of_batches}"
                    )
                    async with self.conn.get_job_db_session() as db:
                        await add_jobs(
                            db,
                            JobType.pow,
                            contexts,
                        )
                logging.info(f"all pow jobs published")

            await asyncio.sleep(self.cooldown)


parser = argparse.ArgumentParser()
parser.add_argument("--batch_size", type=int, action="store", default="1000")
parser.add_argument("--cooldown", type=int, action="store", default="300")
parser.add_argument("--threshold", type=int, action="store", default="500000")
args = parser.parse_args()


def start():
    asyncio.run(
        PowDataJobPublisher(
            batch_size=args.batch_size, cooldown=args.cooldown, threshold=args.threshold
        ).run()
    )
