from functools import wraps

from fastapi.responses import JSONResponse
from fastapi import HTTPException, status


def error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except HTTPException as e:
            return JSONResponse(status_code=e.status_code, content=e.detail)
        except:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content="unknown server error",
            )

    return wrapper
