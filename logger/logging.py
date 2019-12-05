import logging
from .log_formatter import LogFormatter

m_logger = None


def setLogger(logger):
    global m_logger
    m_logger = logger


def getLogger():
    global m_logger
    if m_logger is None:
        return logging.getLogger()
    else:
        return m_logger


def setup_logging(cfg, app_name):
    handler = logging.StreamHandler()
    handler.setFormatter(LogFormatter())
    logging.basicConfig(level=logging.WARNING, handlers=[handler])
    app_logger = logging.getLogger(app_name)
    app_logger.setLevel(cfg.logger.level)
    setLogger(app_logger)
    logging.getLogger('asyncio').setLevel(logging.INFO)
    return app_logger

