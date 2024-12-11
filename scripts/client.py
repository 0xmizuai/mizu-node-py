import argparse
import os

import requests

from mizu_node.security import verify_jwt
from scripts.auth import get_api_keys, issue_api_key, sign_jwt

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

queue_clear_parser = subparsers.add_parser(
    "clear_queue", add_help=False, description="publish jobs"
)
queue_clear_parser.add_argument("--job_type", type=int, action="store")

process_parser = subparsers.add_parser(
    "process", add_help=False, description="process jobs"
)
process_parser.add_argument("--user", type=str, action="store")

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
    elif args.command == "clear_queue":
        service_url = os.environ["NODE_SERVICE_URL"]
        response = requests.post(
            f"{service_url}/clear_queue?job_type={args.job_type}",
            headers={
                "Authorization": f"Bearer {os.environ['API_SECRET_KEY']}",
            },
        )
        response.raise_for_status()
        print(response.json())
    else:
        raise ValueError("Invalid arguments")
