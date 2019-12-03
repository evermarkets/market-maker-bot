import asyncio
import traceback

from strategy.market_maker import MarketMaker
from gateways.emx.adapter import EmxAdapter

from logger import logging

strategies_factory = {
    "market_maker": MarketMaker,
}


class Engine:
    def __init__(self, cfg):
        self.logger = logging.getLogger()

        self.is_active = True
        self.exchange_adapter = EmxAdapter(cfg.adapter)

        try:
            strategy_name = cfg.strategy.name
        except AttributeError:
            self.logger.exception("strategy was not found")
            raise Exception("strategy was not found")

        try:
            self.strategy = strategies_factory[strategy_name](cfg.strategy, self.exchange_adapter)
        except KeyError:
            self.logger.exception("strategy was not found in a factory")
            raise Exception("strategy was not found in a factory")

    async def listen_updates(self):
        while self.is_active:
            try:
                await self.exchange_adapter.listen()
            except Exception as err:
                self.logger.info("listen error: {}".format(err))
                await self.strategy.handle_exception(str(err))
        self.logger.warning("listen_updates was stopped")

    async def run_strategy(self):
        while self.is_active:
            try:
                await self.strategy.run()
            except Exception as err:
                try:
                    await self.strategy.handle_exception(str(err))
                except Exception as err:
                    self.logger.warning("run_strategy handle_exception failed on {}".format(err))
            await asyncio.sleep(0.1)
        self.logger.warning("run_strategy was stopped")

    def run(self):
        self.logger.info("Engine started")

        loop = asyncio.get_event_loop()

        def handle_async_exception(loop_, ctx):
            try:
                self.logger.error("Exception in async task: {0}".format(ctx["exception"]))
            except KeyError:
                self.logger.error("Exception in async task: {0}".format(ctx))

            stack_str = traceback.format_exc()
            self.logger.error("Current traceback: {}".format(stack_str))
            for line in traceback.format_stack():
                self.logger.error(line.strip())

        loop.set_exception_handler(handle_async_exception)

        asyncio.ensure_future(self.exchange_adapter.start())
        asyncio.ensure_future(self.listen_updates())
        asyncio.ensure_future(self.run_strategy())

        loop.set_debug(enabled=True)
        loop.slow_callback_duration = 0.05
        loop.run_forever()
        loop.close()
        self.logger.info("Engine stopped")

    def stop(self):
        self.subscriptions.stop()
