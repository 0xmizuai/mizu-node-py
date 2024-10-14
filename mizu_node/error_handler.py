def get_decorator():
    def decorator(func):
        def new_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ValueError as e:
                return {"error": e.args}
            except:
                return {"error": "unknown"}

        return new_func

    return decorator


error_handler = get_decorator()
