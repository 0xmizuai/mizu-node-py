import os
import pika
from redis import Redis
import time
from functools import wraps
from pika.exceptions import AMQPConnectionError, StreamLostError
import contextlib
from typing import Optional

from mizu_node.types.job import JobType, WorkerJob


ASSIGNED_JOB_EXPIRE_TTL_SECONDS = 900  # 15mins
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672")


def retry_on_connection_error(max_retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            attempts = 0
            while attempts < max_retries:
                try:
                    return func(self, *args, **kwargs)
                except (AMQPConnectionError, StreamLostError) as e:
                    attempts += 1
                    if attempts == max_retries:
                        raise e
                    time.sleep(delay)
                    # Reconnect before retrying
                    self._ensure_connection()
            return None

        return wrapper

    return decorator


class PikaConnectionManager:
    _instance: Optional["PikaConnectionManager"] = None

    def __init__(self):
        self.connection = None

    @classmethod
    def get_instance(cls) -> "PikaConnectionManager":
        if cls._instance is None:
            cls._instance = PikaConnectionManager()
        return cls._instance

    def get_connection(self):
        if self.connection is None or self.connection.is_closed:
            self.connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        return self.connection


class PikaBase(object):
    def __init__(self, qname: str):
        self.qname = qname
        self.connection = PikaConnectionManager.get_instance().get_connection()
        self.channel = self.connection.channel()
        self.queue = self.channel.queue_declare(queue=qname, durable=True)

    def _ensure_connection(self):
        """Ensure the connection is alive and reconnect if necessary"""
        try:
            self.connection = PikaConnectionManager.get_instance().get_connection()
            if not self.channel or self.channel.is_closed:
                self.channel = self.connection.channel()
                self.queue = self.channel.queue_declare(queue=self.qname, durable=True)
        except Exception as e:
            print(f"Error reconnecting to RabbitMQ: {e}")
            raise


class PikaProducer(PikaBase):
    def __init__(self, qname: str):
        super().__init__(qname)

    @retry_on_connection_error()
    def add_item(self, job: WorkerJob):
        self.channel.basic_publish(
            exchange="",
            routing_key=self.qname,
            body=job.model_dump_json(by_alias=True),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )


class PikaConsumer(PikaBase):
    def __init__(self, qname: str):
        super().__init__(qname)

    def _gen_rkey(self, job_id: str) -> str:
        return f"{self.qname}:delivery_tag:{job_id}"

    @retry_on_connection_error()
    def get(self, rclient: Redis) -> WorkerJob | None:
        (method, _, body) = self.channel.basic_get(queue=self.qname, auto_ack=True)
        if method is None:
            return None

        worker_job = WorkerJob.model_validate_json(body)
        rclient.setex(
            self._gen_rkey(worker_job.job_id),
            ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
            str(method.delivery_tag),
        )
        return worker_job

    @retry_on_connection_error()
    def ack(self, rclient: Redis, job_id: int):
        delivery_tag = rclient.get(self._gen_rkey(job_id))
        if delivery_tag:
            self.channel.basic_ack(delivery_tag, False)
        return delivery_tag is not None

    def queue_len(self):
        return self.queue.method.message_count


@contextlib.contextmanager
def job_queue(job_type: JobType, mode: str = "consumer"):
    qname = f"mizu_node:job_queue:{job_type.value}"
    q = PikaProducer(qname) if mode == "producer" else PikaConsumer(qname)
    try:
        yield q
    finally:
        if q.channel and not q.channel.is_closed:
            q.channel.close()
