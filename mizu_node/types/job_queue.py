import os
import pika
from redis import Redis
import time
from functools import wraps
import logging

from mizu_node.types.job import WorkerJob


ASSIGNED_JOB_EXPIRE_TTL_SECONDS = 900  # 15mins
RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672?heartbeat=0"
)


def retry_with_backoff(
    max_retries=3, initial_delay=0.1, max_delay=5.0, exponential_base=2
):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for retry in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (
                    pika.exceptions.AMQPConnectionError,
                    pika.exceptions.AMQPChannelError,
                ) as e:
                    if retry == max_retries - 1:
                        raise
                    logging.warning(
                        f"Connection error, retrying... ({retry + 1}/{max_retries})"
                    )
                    time.sleep(delay)
                    delay = min(delay * exponential_base, max_delay)
                    # Reconnect before retry
                    args[0].ensure_connection()
            return func(*args, **kwargs)

        return wrapper

    return decorator


class PikaBase(object):
    def __init__(self, qname: str):
        self.qname = qname
        self.connection = None
        self.channel = None
        self.queue = None
        self._connect()

    def _connect(self):
        if self.connection is None or self.connection.is_closed:
            self.connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            self.channel = self.connection.channel()
            self.queue = self.channel.queue_declare(queue=self.qname, durable=True)

    def ensure_connection(self):
        try:
            self._connect()
        except Exception as e:
            logging.error(f"Failed to reconnect: {str(e)}")
            raise


class PikaProducer(PikaBase):
    def __init__(self, qname: str):
        super().__init__(qname)

    @retry_with_backoff()
    def add_item(self, item: str):
        self.ensure_connection()
        self.channel.basic_publish(
            exchange="",
            routing_key=self.qname,
            body=item,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )


class PikaConsumer(PikaBase):
    def __init__(self, qname: str):
        super().__init__(qname)

    @retry_with_backoff()
    def get(self) -> str:
        self.ensure_connection()
        (method, _, body) = self.channel.basic_get(queue=self.qname, auto_ack=True)
        return (method.delivery_tag if method else None, body)

    @retry_with_backoff()
    def ack(self, delivery_tag: int):
        self.ensure_connection()
        self.channel.basic_ack(delivery_tag, False)

    @retry_with_backoff()
    def queue_len(self):
        self.ensure_connection()
        return self.queue.method.message_count


class JobQueueV2(object):
    def __init__(self, qname: str):
        self.qname = qname
        self.producer = PikaProducer(qname)
        self.consumer = PikaConsumer(qname)

    def add_item(self, job: WorkerJob):
        self.producer.add_item(job.model_dump_json(by_alias=True))

    def _gen_rkey(self, job_id: str) -> str:
        return f"{self.qname}:delivery_tag:{job_id}"

    def get(self, rclient: Redis) -> WorkerJob | None:
        (delivery_tag, job_json) = self.consumer.get()
        if delivery_tag is None:
            return None

        worker_job = WorkerJob.model_validate_json(job_json)
        rclient.setex(
            self._gen_rkey(worker_job.job_id),
            ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
            str(delivery_tag),
        )
        return worker_job

    def ack(self, rclient: Redis, job_id: int):
        delivery_tag = rclient.get(self._gen_rkey(job_id))
        if delivery_tag:
            self.consumer.ack(int(delivery_tag))
        return delivery_tag is not None

    def queue_len(self):
        return self.consumer.queue_len()
