import datetime
from functools import wraps
import os
import time
import traceback

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel


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


def build_json_response(
    status_code: status, message: str, data: BaseModel = None
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "message": message,
            "data": data.model_dump(by_alias=True, exclude_none=True) if data else {},
        },
    )


def build_ok_response(data: BaseModel = None) -> JSONResponse:
    return build_json_response(status.HTTP_200_OK, "ok", data)


def epoch():
    # use datetime here so we can use freezetime in tests
    d = datetime.datetime.now()
    return int(time.mktime(d.timetuple()))


def epoch_ms():
    return int(time.time() * 1000)


def is_prod():
    return os.environ.get("RAILWAY_ENVIRONMENT_NAME", "dev") == "production"
