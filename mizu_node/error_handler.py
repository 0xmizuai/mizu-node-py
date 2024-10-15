from functools import wraps


def error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValueError as e:
            return {"error": e.args}
        except:
            return {"error": "unknown"}

    return wrapper
