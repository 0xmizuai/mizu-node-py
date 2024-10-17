from mizu_node.error_handler import error_handler


@error_handler
def do_not_pass(error: ValueError | Exception | None):
    if error is None:
        return "ok"
    else:
        raise error


def test_error_handler():
    assert do_not_pass(None) == "ok"
    assert do_not_pass(ValueError("arg1")) == {"error": ("arg1",)}
    assert do_not_pass(Exception("arg2")) == {"error": "unknown"}
