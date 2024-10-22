from functools import wraps

from fastapi.responses import JSONResponse
from fastapi import HTTPException, status


def error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except HTTPException as e:
            return JSONResponse(status=e.status_code, message=e.detail)
        except:
            return JSONResponse(
                status=status.HTTP_500_INTERNAL_SERVER_ERROR, message="unknown error"
            )

    return wrapper
