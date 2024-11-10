import contextlib
import os
import pika
from redis import Redis
import time
from functools import wraps
import logging
import threading

from mizu_node.types.job import WorkerJob


ASSIGNED_JOB_EXPIRE_TTL_SECONDS = 1800 - 60  # < 30mins
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


class RabbitMQConnection:
    _producer_instance = None
    _consumer_instance = None
    _producer_connection = None
    _consumer_connection = None
    _lock = threading.Lock()

    def __new__(cls, connection_type: str):
        if connection_type == "producer":
            if cls._producer_instance is None:
                with cls._lock:
                    if cls._producer_instance is None:
                        cls._producer_instance = super().__new__(cls)
            return cls._producer_instance
        elif connection_type == "consumer":
            if cls._consumer_instance is None:
                with cls._lock:
                    if cls._consumer_instance is None:
                        cls._consumer_instance = super().__new__(cls)
            return cls._consumer_instance
        raise ValueError("Invalid connection type")

    def __init__(self, connection_type: str):
        self.connection_type = connection_type

    def get_connection(self):
        if self.connection_type == "producer":
            if self._producer_connection is None or self._producer_connection.is_closed:
                self._producer_connection = pika.BlockingConnection(
                    pika.URLParameters(RABBITMQ_URL)
                )
            return self._producer_connection
        else:
            if self._consumer_connection is None or self._consumer_connection.is_closed:
                self._consumer_connection = pika.BlockingConnection(
                    pika.URLParameters(RABBITMQ_URL)
                )
            return self._consumer_connection

    def close(self):
        if self.connection_type == "producer":
            if self._producer_connection and not self._producer_connection.is_closed:
                self._producer_connection.close()
                self._producer_connection = None
        else:
            if self._consumer_connection and not self._consumer_connection.is_closed:
                self._consumer_connection.close()
                self._consumer_connection = None


class PikaBase(object):
    def __init__(self, qname: str, connection_type: str):
        self.qname = qname
        self.channel = None
        self.queue = None
        self.connection_manager = RabbitMQConnection(connection_type)
        self._connect()

    def _connect(self):
        if self.channel is None or self.channel.is_closed:
            connection = self.connection_manager.get_connection()
            self.channel = connection.channel()
            self.queue = self.channel.queue_declare(queue=self.qname, durable=True)

    def ensure_connection(self):
        try:
            self._connect()
        except Exception as e:
            logging.error(f"Failed to reconnect: {str(e)}")
            raise

    def close(self):
        """Gracefully close the channel"""
        if self.channel and not self.channel.is_closed:
            try:
                self.channel.close()
            except Exception as e:
                logging.error(f"Error closing channel: {str(e)}")
            finally:
                self.channel = None
                self.queue = None

    def __del__(self):
        self.close()


class PikaProducer(PikaBase):
    def __init__(self, qname: str):
        super().__init__(qname, "producer")

    @retry_with_backoff()
    def add_item(self, job: WorkerJob):
        self.ensure_connection()
        self.channel.basic_publish(
            exchange="",
            routing_key=self.qname,
            body=job.model_dump_json(by_alias=True),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )


class PikaConsumer(PikaBase):
    def __init__(self, qname: str):
        super().__init__(qname, "consumer")

    def _gen_rkey(self, job_id: str) -> str:
        return f"{self.qname}:delivery_tag:{job_id}"

    @retry_with_backoff()
    def get(self, rclient: Redis) -> WorkerJob | None:
        self.ensure_connection()
        (method, _, body) = self.channel.basic_get(queue=self.qname, auto_ack=False)
        if method is None:
            return None

        worker_job = WorkerJob.model_validate_json(body)
        rclient.setex(
            self._gen_rkey(worker_job.job_id),
            ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
            str(method.delivery_tag),
        )
        return worker_job

    @retry_with_backoff()
    def ack(self, rclient: Redis, job_id: str):
        self.ensure_connection()
        delivery_tag = rclient.get(self._gen_rkey(job_id))
        if delivery_tag:
            self.channel.basic_ack(delivery_tag, False)
        return delivery_tag is not None

    @retry_with_backoff()
    def queue_len(self):
        self.ensure_connection()
        return self.queue.method.message_count

    def queue_len(self):
        return self.consumer.queue_len()


@contextlib.contextmanager
def job_queue(qname: str, connection_type: str = "consumer"):
    queue = None
    try:
        if connection_type == "producer":
            queue = PikaProducer(qname)
        else:
            queue = PikaConsumer(qname)
        yield queue
    finally:
        if queue:
            queue.close()
