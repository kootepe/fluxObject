import logging
import logging.config


def init_logger(log_level=None):
    """Custom logger that has .ini name in the logging message"""

    logging.config.fileConfig('logging.ini')
    logger = logging.getLogger("defaultLogger")
    if log_level:
        logger.setLevel(getattr(logging, log_level))

    return logger
