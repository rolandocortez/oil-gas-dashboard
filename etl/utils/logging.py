# Simple logger helper for ETL scripts.
import logging
import sys


def get_logger(name: str = "oilgas_etl") -> logging.Logger:
    fmt = "[%(asctime)s] %(levelname)s %(name)s: %(message)s"
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
    logger.propagate = False
    return logger
