import argparse
import os

import requests

from mizu_node.security import verify_jwt
from publisher.classifier import list_classifiers, register_classifier
from scripts.auth import get_api_keys, issue_api_key, sign_jwt
from scripts.importer import (
    CommonCrawlWetMetadataUploader,
    import_to_r2,
    migrate_metadata,
)

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="command", required=True)


new_api_key_parser = subparsers.add_parser(
    "new_api_key",
    add_help=False,
)
new_api_key_parser.add_argument(
    "--user", action="store", type=str, help="User to issue new API key"
)

get_api_keys_parser = subparsers.add_parser(
    "get_api_keys",
    add_help=False,
)
get_api_keys_parser.add_argument(
    "--user", action="store", type=str, help="User to query API keys"
)

new_jwt_parser = subparsers.add_parser(
    "new_jwt",
    add_help=False,
)
new_jwt_parser.add_argument("--user", action="store", type=str, help="user id to sign")

verify_jwt_parser = subparsers.add_parser(
    "verify_jwt",
    add_help=False,
)
verify_jwt_parser.add_argument(
    "--token", action="store", type=str, help="the token to verify"
)

import_parser = subparsers.add_parser(
    "import", add_help=False, description="import data to r2"
)
import_parser.add_argument("--range", type=str, action="store", help="e.g 10,20")
import_parser.add_argument(
    "--source", type=str, action="store", default="s3", help="data source"
)
import_parser.add_argument(
    "--pathfile", type=str, action="store", help="paths file to download"
)

metadata_parser = subparsers.add_parser(
    "metadata", add_help=False, description="backup metadata to r2"
)
metadata_parser.add_argument(
    "--backup", type=str, action="store", help="the batch to backup"
)
metadata_parser.add_argument(
    "--restore", type=str, action="store", help="the batch to restore"
)

publish_parser = subparsers.add_parser(
    "publish", add_help=False, description="publish jobs"
)
publish_parser.add_argument("--user", type=str, action="store")
publish_parser.add_argument("--batch", type=str, action="store")
publish_parser.add_argument("--classifier", type=str, action="store")

queue_clear_parser = subparsers.add_parser(
    "clear_queue", add_help=False, description="publish jobs"
)
queue_clear_parser.add_argument("--job_type", type=int, action="store")

process_parser = subparsers.add_parser(
    "process", add_help=False, description="process jobs"
)
process_parser.add_argument("--user", type=str, action="store")

classifier_parser = subparsers.add_parser(
    "classifier", add_help=False, description="import data to r2"
)
classifier_parser.add_argument("--register", action="store_true")
classifier_parser.add_argument("--list", action="store_true")
classifier_parser.add_argument("--user", type=str, action="store")

migrate_parser = subparsers.add_parser(
    "migrate", add_help=False, description="migrate metadata to mongodb"
)

args = parser.parse_args()


def main():
    if args.command == "new_api_key":
        key = issue_api_key(args.user)
        print("API key: " + key)
    elif args.command == "get_api_keys":
        keys = get_api_keys(args.user)
        for key in keys:
            print("API key: " + key)
    elif args.command == "new_jwt":
        token = sign_jwt(args.user, os.environ["JWT_SECRET_KEY"])
        print("Token: " + token)
    elif args.command == "verify_jwt":
        user = verify_jwt(args.token, os.environ["JWT_VERIFY_KEY"])
        print("User: " + user)
    elif args.command == "import":
        [start, end] = [int(i) for i in args.range.split(",")]
        import_to_r2(args.source, args.pathfile, start, end)
    elif args.command == "metadata":
        if args.backup:
            CommonCrawlWetMetadataUploader(args.backup).iterate_and_upload()
        elif args.restore:
            raise ValueError("command not implemented yet")
        else:
            raise ValueError("either backup or restore must be presented")
    elif args.command == "migrate":
        migrate_metadata()
    elif args.command == "classifier":
        if args.register:
            register_classifier(args.user)
        elif args.list:
            list_classifiers(args.user)
        else:
            raise ValueError("Invalid arguments")
    elif args.command == "clear_queue":
        service_url = os.environ["NODE_SERVICE_URL"]
        response = requests.post(
            f"{service_url}/clear_queue?job_type={args.job_type}",
            headers={
                "Authorization": f"Bearer {os.environ['MIZU_ADMIN_USER_API_KEY']}"
            },
        )
        response.raise_for_status()
        print(response.json())
    else:
        raise ValueError("Invalid arguments")
