import time
import traceback

from .strategy_interface import strategy_interface
from orders_manager import OrdersManager

from logger import logging

from definitions import (
    tob,
    order_request,
    order_type,
    order_side,
    exchange_orders,
    new_order_rejection,
    amend_rejection,
)


class MarketMaker(strategy_interface):
    TIME_TO_WAIT_SINCE_START_SECS = 10
    MAX_NUMBER_OF_ATTEMPTS_SECS = 5

    def __init__(self, cfg, exchange_adapter):
        self.logger = logging.getLogger()

        self._load_configuration(cfg)

        self.config = cfg
        self.exchange_adapter = exchange_adapter
        self.exchange_adapter.set_order_update_callback(self.on_market_update)
        self.orders_manager = OrdersManager(self.exchange_adapter)

        self.exchange_adapter.cancel_orders_on_start = True

        self.update_orders = False

        self.started_time = time.time()
        self.last_amend_time = None
        self.reconnecting = False

        self.active = True
        self.tob = None
        self.num_of_sent_orders = 0
        self.cancel_all_request_was_sent = False

        self.user_asks = self.config.orders.asks
        self.user_bids = self.config.orders.bids

    def _load_configuration(self, cfg):
        option_names = (
            'instrument_name',
            'mid_price_based_calculation',
            'tick_size',
            'price_rounding',
            'stop_strategy_on_error',
        )
        for option_name in option_names:
            option = getattr(cfg, option_name)
            if option is None:
                self.logger.error(f'{option_name} was not found')
                raise Exception(f'{option_name} was not found')
            setattr(self, option_name, option)

    async def handle_exception(self, err_msg):
        self.logger.error(f'handle_exception traceback: {err_msg}')
        for line in traceback.format_stack():
            self.logger.error(line.strip())

        stack_str = traceback.format_stack()
        self.logger.error(f'additional handle_exception traceback: {stack_str}')

        count = 0
        while count < 5:
            try:
                await self._handle_exception(err_msg)
                return True
            except Exception as err:
                self.logger.exception(f'Exception raised {err}')
                err_msg = err

            count += 1
            self.logger.warning('reconnection failed, performing new attempt')
        raise Exception(f'handle_exception was unsuccessfully tried 5 times')

    async def _handle_exception(self, err_msg):
        self.logger.warning(f'Gateway will be reconnected because of {err_msg}')
        if self.stop_strategy_on_error is True:
            await self.stop_strategy()

        self.reconnecting = True

        await self.reset(err_msg)
        self.started_time = time.time()

        self.reconnecting = False

        self.logger.warning(f'Gateway was reconnected because of {err_msg}')
        return True

    async def stop_strategy(self):
        try:
            self.logger.info('Cancelling orders because strategy is stopped')
            await self._cancel_orders()
        except Exception as err:
            self.logger.warning(f'stop_strategy failed on {err}')
            raise

        self.active = False

    async def reset(self, reset_reason):
        self.cancel_all_request_was_sent = False

        await self._cancel_orders()
        self.cancel_all_request_was_sent = True
        self.last_amend_time = None
        self.num_of_sent_orders = 0
        await self.exchange_adapter.reconnect()

    async def _cancel_orders(self):
        try:
            await self.orders_manager.cancel_active_orders()
        except Exception as err:
            res = await self.handle_exception(err)
            if res is False:
                self.logger.exception(f'_cancel_orders method failed, msg {err}')
                raise Exception(f'_cancel_orders method failed, msg {err}')
            return

    async def process_active_orders_on_start(self, orders_msg):
        if len(orders_msg.bids + orders_msg.asks) == 0:
            return
        elif len(orders_msg.bids + orders_msg.asks) % 2 != 0:
            await self._cancel_orders()
            return

        self.orders_manager.activate_orders(orders_msg)

    async def on_market_update(self, update):
        if self.active is False:
            self.logger.info('Strategy is not active, update will be ignored')
            return
        elif isinstance(update, tob):
            if self.tob is None:
                self.update_orders = True
                self.tob = update
            elif self.tob_moved(update):
                self.update_orders = True
                self.tob = update
            return
        elif isinstance(update, exchange_orders):
            await self.process_active_orders_on_start(update)
            return
        elif isinstance(update, (amend_rejection, new_order_rejection)):
            self.logger.info(f'Received order rejection {update.__dict__}')

        try:
            self.orders_manager.update_order_state(update.order_id, update)
        except Exception as err:
            self.logger.error(f'update_order_state failed on {update}')
            raise Exception(f'on_market_update raised. update = {type(update)}, reason = {err}')

    async def run(self):
        if self.active is False:
            self.logger.info('Strategy is not active, method run will be stopped')
            return
        elif self.tob is None:
            return
        elif self.update_orders is False:
            return
        elif self.started_time + self.TIME_TO_WAIT_SINCE_START_SECS > time.time():
            return

        self.update_orders = False
        await self.process_market_move()

    def tob_moved(self, tob):
        if self.tob.best_bid_price != tob.best_bid_price or \
                self.tob.best_ask_price != tob.best_ask_price:
            return True
        return False

    def _orders_are_ready_for_amend(self):
        known_statuses = self.orders_manager.get_number_of_ready_for_amend()
        if self.last_amend_time and len(self.orders_manager.live_orders_ids) > 0 and \
                known_statuses != self.num_of_sent_orders:
            return known_statuses
        return True

    def generate_orders(self):
        best_ask, best_bid = self.tob.best_ask_price, self.tob.best_bid_price
        if self.mid_price_based_calculation:
            mid_price = (self.tob.best_ask_price + self.tob.best_bid_price) / 2.0
            rounded_mid = round(round(mid_price / self.tick_size) * self.tick_size,
                                self.price_rounding)

            if best_ask - best_bid == 2.0 * self.tick_size:
                best_ask = round(rounded_mid + self.tick_size, self.price_rounding)
                best_bid = round(rounded_mid - self.tick_size, self.price_rounding)
            elif rounded_mid >= mid_price:
                best_ask = rounded_mid
                best_bid = round(best_ask - self.tick_size, self.price_rounding)
            else:
                best_bid = rounded_mid
                best_ask = round(best_bid + self.tick_size, self.price_rounding)

        orders = []
        for quote in self.user_asks:
            level, qty = quote
            order = order_request()
            order.instrument_name = self.config.instrument_name
            order.side = order_side.sell
            order.type = order_type.limit

            order.price = round(best_ask + self.tick_size * level, self.price_rounding)
            order.quantity = qty
            orders.append(order)

        for quote in self.user_bids:
            level, qty = quote
            order = order_request()
            order.instrument_name = self.config.instrument_name
            order.side = order_side.buy
            order.type = order_type.limit

            order.price = round(best_bid - self.tick_size * level, self.price_rounding)
            order.quantity = qty
            orders.append(order)
        return orders

    async def process_market_move(self):
        if self.active is False:
            self.logger.info('Strategy is not active, process_market_move will be stopped')
            return
        elif self.reconnecting is True:
            self.logger.info('Ongoing reconnection, process_market_move will be stopped')
            return

        self.logger.info('process_market_move started')

        res = self._orders_are_ready_for_amend()
        if res is not True:

            self.logger.info('_orders_are_ready_for_amend returned False')

            known_statuses = res
            if self.last_amend_time + self.MAX_NUMBER_OF_ATTEMPTS_SECS < time.time():
                err_msg = (
                    f'Will be reconnected since only {known_statuses} '
                    f'active orders were updated within {self.MAX_NUMBER_OF_ATTEMPTS_SECS} seconds'
                )

                res = await self.handle_exception(err_msg)
                if res is False:
                    self.logger.log('Error: %s', err_msg)
                    raise Exception('handle_exception failed')
                return
            return

        orders = self.generate_orders()

        try:
            await self.orders_manager.amend_active_orders(orders)
        except Exception as err:
            res = await self.handle_exception(err)
            if res is False:
                self.logger.exception('Exception')
                raise Exception(f'Orders amend failed {err}')
            return

        self.last_amend_time = time.time()
        self.num_of_sent_orders = len(orders)
