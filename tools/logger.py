import logging
import logging.config


def init_logger():
    """Custom logger that has .ini name in the logging message"""

    logging.config.fileConfig('logging.ini')
    # logger = logging.LoggerAdapter(logging.config.fileConfig(
    #     'logging.ini'), {'ini': 'ini_name'})

    logger = logging.getLogger("defaultLogger")
    # logger = logging.LoggerAdapter(logger, {'ini': 'ini_name'})

    return logger
