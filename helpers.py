

__all__ = [
    "counter",
]


def counter(func):
    cnt = 0

    def inner(*args, **kwargs):
        nonlocal cnt
        kwargs["_ElectricityDB__postfix"] = cnt
        cnt += 1
        return func(*args, **kwargs)
    return inner
