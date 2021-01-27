

__all__ = [
    "counter",
    "HOUR_COEFFICIENTS",
]


def counter(func):
    cnt = 0

    def inner(*args, **kwargs):
        nonlocal cnt
        kwargs["_ElectricityDB__postfix"] = cnt
        cnt += 1
        return func(*args, **kwargs)
    return inner


HOUR_COEFFICIENTS = {
    '00': 0.5,
    '01': 0.5,
    '02': 0.5,
    '03': 0.5,
    '04': 0.5,
    '05': 0.5,
    '06': 0.5,
    '07': 1,
    '08': 2,
    '09': 2,
    '10': 3,
    '11': 3,
    '12': 3,
    '13': 3,
    '14': 3,
    '15': 3,
    '16': 3,
    '17': 3,
    '18': 3,
    '19': 3,
    '20': 3,
    '21': 2,
    '22': 2,
    '23': 1,
}
