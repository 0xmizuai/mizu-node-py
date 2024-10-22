import pprint
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from mizu_node.error_handler import error_handler
from fastapi import status


@error_handler
def do_not_pass(error: HTTPException | Exception | None):
    if error is None:
        return "ok"
    else:
        raise error


def test_error_handler():
    assert do_not_pass(None) == "ok"
    e = HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="invalid job"
    )
    result1: JSONResponse = do_not_pass(e)
    result1.status_code = e.status_code
    result1.content = e.detail

    result2: JSONResponse = do_not_pass(Exception("arg2"))
    result2.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    result2.content = "unknown server error"
