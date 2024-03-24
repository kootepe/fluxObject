import logging
import logging.config

import logging

class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        # Original formatTime provides time up to seconds
        original_formatted_time = super().formatTime(record, datefmt="%Y-%m-%d %H:%M:%S")
        # Add milliseconds with a dot as a separator
        formatted_time = f"{original_formatted_time}.{int(record.msecs):03d}"
        return formatted_time


def init_logger(log_level=None):
    """
    Initiates logger from logging.ini and optionally takes logging level
    from .ini
    """

    logging.config.fileConfig('logging.ini')
    logger = logging.getLogger("defaultLogger")
    if log_level:
        logger.setLevel(getattr(logging, log_level.upper()))
    for handler in logger.handlers:
        handler.setFormatter(CustomFormatter(handler.formatter._fmt))

    return logger
