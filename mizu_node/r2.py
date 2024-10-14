import boto3

from mizu_node.constants import R2_ACCESS_KEY, R2_BUCKET, R2_ENDPOINT, R2_SECRET_KEY

s3 = boto3.resource(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)


def read_raw_data(key: str):
    obj = s3.Object(R2_BUCKET, key)
    return obj.get()["Body"].read().decode("utf-8")


def save_raw_data(key: str, data: str):
    obj = s3.Object(R2_BUCKET, key)
    obj.put(Body=data.encode("utf-8"))
