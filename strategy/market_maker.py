import asyncio
import traceback

from .strategy_interface import strategy_interface

from logger import logging

class market_maker(strategy_interface):

    def __init__(self, cfg, exchange_adapter):
        self.logger = logging.getLogger()

        self._load_configuration(cfg)

        self.config = cfg
        self.exchange_adapter = exchange_adapter
        self.exchange_adapter.set_order_update_callback(self.on_market_update)

        self.cancel_all_request_was_sent = False

    def _load_configuration(self, cfg):
        option_names = (
            "instrument_name",
            "tick_size",
            "stop_strategy_on_error",
            "cancel_orders_on_reconnection",
        )
        for option_name in option_names:
            option = getattr(cfg, option_name)
            if option is None:
                self.logger.error("%s was not found", option_name)
                raise Exception("{0} was not found".format(option_name))
            setattr(self, option_name, option)

    async def handle_exception(self, err_msg):

        import pdb; pdb.set_trace()

        #TODO check this
        self.logger.error("handle_exception traceback: {}".format(err_msg))
        for line in traceback.format_stack():
            self.logger.error(line.strip())

        stack_str = traceback.format_stack()
        self.logger.error("additional handle_exception traceback: {}".format(stack_str))

        count = 0
        while count < 5:
            try:
                await self._handle_exception(err_msg, exchange_name, self.stop_strategy_on_error)
                return True
            except Exception as err:
                self.logger.exception("{} Exception raised {}".format(err))
                err_msg = err

            count += 1
            self.logger.warning("reconnection failed, performing new attempt")
        raise Exception("{}, handle_exception was unsuccessflully tried 5 times".format(get_filename_and_lineno()))

    async def _handle_exception(self, err_msg, stop_strategy):
        self.logger.warning("{} Gateway will be reconnected because of {}".format(err_msg))
        if stop_strategy is True:
            await self.stop_strategy()

        self.reconnecting = True

        await self.reset(err_msg)
        self.started_time = time.time()

        self.reconnecting = False

        self.logger.warning("{} Gateway was reconnected because of {}".format(err_msg))
        return True

    async def reset(self, reset_reason):
        self.cancel_all_request_was_sent = False

        if self.cancel_orders_on_reconnection:
            await self._cancel_orders()
            self.cancel_all_request_was_sent = True
            self.last_amend_time = None
            self.num_of_sent_orders = 0
            self.primary_ob = None

        await self.exchange_adapter.reconnect()

    async def on_market_update(self, update):
        pass

    async def run(self):
        pass
