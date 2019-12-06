import time
import traceback
from enum import Enum

from strategy.strategy_interface import StrategyInterface
from orders_manager import OrdersManager

from logger import logging

from definitions import (
    TopOfBook,
    OrderRequest,
    OrderType,
    OrderSide,
    ExchangeOrders,
    NewOrderRejection,
    AmendRejection,
    Position,
    OrderEliminationAcknowledgement,
)


class ActionType(Enum):
    Nothing = 1
    IgnoreReconnection = 2


class MarketMaker(StrategyInterface):
    TIME_TO_WAIT_SINCE_START_SECS = 10
    MAX_NUMBER_OF_ATTEMPTS_SECS = 5

    def __init__(self, cfg, exchange_adapter):
        self.logger = logging.getLogger()

        self._load_configuration(cfg)

        self.config = cfg
        self.exchange_adapter = exchange_adapter
        self.exchange_adapter.set_order_update_callback(self.on_market_update)
        self.orders_manager = OrdersManager(self.exchange_adapter)

        self.process_orders_on_start = False
        self.exchange_adapter.cancel_orders_on_start = True

        self.update_orders_flag = False

        self.started_time = time.time()
        self.last_amend_time = None
        self.reconnecting = False

        self.tob = None
        self.active = True
        self.current_position = None
        self.num_of_sent_orders = 0
        self.cancel_all_request_was_sent = False
        self.positional_retreat_increment = self.positional_retreat.position_increment
        self.positional_retreat_ticks = self.positional_retreat.retreat_ticks

        self.user_asks = self.config.orders.asks
        self.user_bids = self.config.orders.bids

        self.white_list = {
            "post-only order would cross as non-maker": ActionType.IgnoreReconnection,
        }

    def _load_configuration(self, cfg):
        option_names = (
            'instrument_name',
            'mid_price_based_calculation',
            'tick_size',
            'price_rounding',
            'stop_strategy_on_error',
            'positional_retreat'
        )
        for option_name in option_names:
            option = getattr(cfg, option_name)
            if option is None:
                self.logger.error(f'{option_name} was not found')
                raise Exception(f'{option_name} was not found')
            setattr(self, option_name, option)

    def should_perform_positional_retreat(self):
        return not( not self.positional_retreat_increment and not self.positional_retreat_ticks)

    async def handle_exception(self, err_msg):
        for whitelisted_msg in self.white_list.keys():
            if whitelisted_msg in str(err_msg) and \
                    self.white_list[whitelisted_msg] is ActionType.IgnoreReconnection:
                self.logger.info(
                    "Error was whitelisted, reconnection won't be performed. Err: {}".format(
                        err_msg))
                return

        stack_str = traceback.format_exc()
        self.logger.error("handle_exception traceback: {}".format(stack_str))
        for line in traceback.format_stack():
            self.logger.error(line.strip())

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
        self.orders_manager.reset()
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
        if not self.process_orders_on_start:
            return
        self.orders_manager.activate_orders(orders_msg)

    async def on_market_update(self, update):
        if self.active is False:
            self.logger.info('Strategy is not active, update will be ignored')
            return
        elif isinstance(update, TopOfBook):
            if self.tob is None:
                self.update_orders_flag = True
                self.tob = update
            elif self.tob_moved(update):
                self.update_orders_flag = True
                self.tob = update
            return
        elif isinstance(update, ExchangeOrders):
            await self.process_active_orders_on_start(update)
            return
        elif isinstance(update, Position):
            self.current_position = update.position
            return
        elif isinstance(update, (AmendRejection, NewOrderRejection)):
            self.logger.info(f'Received order rejection {update.__dict__}')
            raise Exception(f'Received order rejection {update.__dict__}')
        elif isinstance(update, OrderEliminationAcknowledgement):
            self.logger.info(f'Received order elimination {update.__dict__}')
            raise Exception(f'Received order elimination {update.__dict__}')

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
        elif self.update_orders_flag is False:
            return
        elif self.started_time + self.TIME_TO_WAIT_SINCE_START_SECS > time.time():
            return

        self.update_orders_flag = False
        await self.react_to_market_move()

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
            order = OrderRequest()
            order.instrument_name = self.config.instrument_name
            order.side = OrderSide.sell
            order.type = OrderType.limit

            order.price = round(best_ask + self.tick_size * level, self.price_rounding)
            order.quantity = qty
            orders.append(order)

        for quote in self.user_bids:
            level, qty = quote
            order = OrderRequest()
            order.instrument_name = self.config.instrument_name
            order.side = OrderSide.buy
            order.type = OrderType.limit

            order.price = round(best_bid - self.tick_size * level, self.price_rounding)
            order.quantity = qty
            orders.append(order)
        return orders

    def perform_retreats(self, orders):
        bids_ = [order for order in orders if order.side == OrderSide.buy]
        asks_ = [order for order in orders if order.side == OrderSide.sell]

        if self.current_position is None:
            return None

        retreat_in_ticks = int(self.current_position / self.positional_retreat_increment) * \
                           self.positional_retreat_ticks
        if retreat_in_ticks == 0:
            # position is insufficient for retreating
            return orders

        if retreat_in_ticks > 0.0:
            for order in bids_:
                order.price = round(order.price - self.tick_size * retreat_in_ticks,
                                    self.price_rounding)
        else:
            for order in asks_:
                order.price = round(order.price - self.tick_size * retreat_in_ticks,
                                    self.price_rounding)
        return bids_ + asks_

    async def react_to_market_move(self):
        if self.active is False:
            self.logger.info('Strategy is not active, react_to_market_move will be stopped')
            return
        elif self.reconnecting is True:
            self.logger.info('Ongoing reconnection, react_to_market_move will be stopped')
            return

        self.logger.info('react_to_market_move started')

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
        if self.should_perform_positional_retreat():
            orders = self.perform_retreats(orders)
            if not orders:
                self.logger.warning('Failed to perform retreat adjustment')
                return

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
