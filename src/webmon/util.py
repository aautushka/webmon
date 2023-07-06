from datetime import datetime


def now() -> float:
    return datetime.utcnow().timestamp()
