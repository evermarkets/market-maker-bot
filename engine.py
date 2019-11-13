import asyncio
import traceback
from strategy.maker import maker

from logger import logging

strategies_factory = {
    "maker": maker,
}


class Engine:
    def __init__(self, cfg):
        self.logger = logging.getLogger()

    def run(self):
        self.logger.info("Engine is strated")
