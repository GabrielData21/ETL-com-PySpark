from .logger import get_logger

def get_spark_session(*args, **kwargs):
    from .spark_session import get_spark_session as _get_spark_session
    return _get_spark_session(*args, **kwargs)

__all__ = ["get_spark_session", "get_logger"]
