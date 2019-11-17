import asyncio
import traceback

from strategy.market_maker import market_maker
from gateways.emx.adapter import emx_adapter

from logger import logging

strategies_factory = {
    "market_maker": market_maker,
}


class Engine:
    def __init__(self, cfg):
        self.logger = logging.getLogger()

        self.exchange_adapter = emx_adapter(cfg.adapter)

    def run(self):
        self.logger.info("Engine is strated")
