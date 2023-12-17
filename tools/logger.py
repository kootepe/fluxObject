import logging
import logging.config


def init_logger(log_level=None):
    """
    Initiates logger from logging.ini and optionally takes logging level
    from .ini
    """

    logging.config.fileConfig('logging.ini')
    logger = logging.getLogger("defaultLogger")
    if log_level:
        logger.setLevel(getattr(logging, log_level))

    return logger
