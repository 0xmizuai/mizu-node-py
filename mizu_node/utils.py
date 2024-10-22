import time

from fastapi.responses import JSONResponse
from fastapi import status


def epoch():
    return int(time.time())


def build_json_response(
    status_code: status, message: str, data: dict = {}
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code, content={"message": message, "data": data}
    )
