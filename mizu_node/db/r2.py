import boto3
import os

ACCOUTN_ID = os.environ["R2_ACCOUNT_ID"]
R2_ENDPOINT = f"https://{ACCOUTN_ID}.r2.cloudflarestorage.com"
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]

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
