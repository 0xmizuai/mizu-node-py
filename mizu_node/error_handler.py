from functools import wraps

from fastapi.responses import JSONResponse
from fastapi import status


class MizuError(Exception):
    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message


def error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except MizuError as e:
            return JSONResponse(status=e.status, content=e.message)
        except:
            return JSONResponse(
                status=status.HTTP_500_INTERNAL_SERVER_ERROR, content="unknown error"
            )

    return wrapper
