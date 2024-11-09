import os
import pika
from redis import Redis

from mizu_node.types.job import WorkerJob


ASSIGNED_JOB_EXPIRE_TTL_SECONDS = 900  # 15mins
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672")


class PikaBase(object):
    def __init__(self, qname: str):
        self.qname = qname
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        self.channel = connection.channel()
        self.queue = self.channel.queue_declare(queue=qname, durable=True)


class PikaProducer(PikaBase):
    def __init__(self, qname: str):
        super().__init__(qname)

    def add_item(self, item: str):
        self.channel.basic_publish(
            exchange="",
            routing_key=self.qname,
            body=item,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )


class PikaConsumer(PikaBase):
    def __init__(self, qname: str):
        super().__init__(qname)

    def get(self) -> str:
        (method, _, body) = self.channel.basic_get(queue=self.qname, auto_ack=True)
        return (method.delivery_tag if method else None, body)

    def ack(self, delivery_tag: int):
        self.channel.basic_ack(delivery_tag, False)

    def queue_len(self):
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
