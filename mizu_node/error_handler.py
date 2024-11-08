from functools import wraps
import traceback

from fastapi import HTTPException, status

from mizu_node.utils import build_json_response


def error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except HTTPException as e:
            return build_json_response(e.status_code, e.detail)
        except Exception as e:
            print(traceback.format_exc())
            return build_json_response(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "unknown server error",
            )

    return wrapper
