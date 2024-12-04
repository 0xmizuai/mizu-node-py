import logging

import functools
from typing import Callable, TypeVar, ParamSpec


logging.basicConfig(level=logging.INFO)  # Set the desired logging level

T = TypeVar("T")
P = ParamSpec("P")


def with_transaction(func: Callable[P, T]) -> Callable[P, T]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        db = args[0]  # Get the actual db connection
        try:
            result = func(*args, **kwargs)
            db.commit()
            return result
        except Exception as e:
            db.rollback()
            logging.error(f"Transaction failed in {func.__name__}: {e}")
            raise

    return wrapper
