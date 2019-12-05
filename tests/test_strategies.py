import json
import pytest
import datetime
from munch import DefaultMunch

from strategy.market_maker import MarketMaker
from gateways import gateway_interface

from definitions import (
    tob,
    new_order_ack,
    new_order_rejection,
    order_elim_ack,
    order_elim_rejection,
    order_fill_ack,
    order_full_fill_ack,
)


class BittestStorage():
    def __init__(self):
        self.uid_to_eid = {}
        self.eid_to_uid = {}


class BittestAdapter(gateway_interface.GatewayInterface):

    def __init__(self):
        super().__init__()

        self.is_ready_flag = True
        self.storage = BittestStorage()
        self.config = DefaultMunch()
        self.config.name = "bittest"
        self.orders_sent = 0
        self.orders_amended = 0
        self.orders_cancelled = 0
        self.amend_orders_data = []
        self.new_orders_data = []

    def set_order_update_callback(self, callback):
        pass

    async def send_order(self, order_request):
        res = api_result()
        res.success = True

        self.orders_sent += 1
        self.new_orders_data.append(order_request)
        return res

    async def send_orders(self, orders_request):
        res = api_result()
        res.success = True

        self.orders_sent += len(orders_request)
        for _request in orders_request:
            self.new_orders_data.append(_request)
        return res

    async def amend_orders(self, new, old):
        res = api_result()
        res.success = True

        self.orders_amended += len(new)
        return res

    async def amend_order(self, i, j):
        res = api_result()
        res.success = True

        self.orders_amended += 1
        self.amend_orders_data.append((i, j))
        return res

    async def cancel_order(self, cancel_request):
        res = api_result()
        res.success = True

        self.orders_cancelled += 1
        return res

    async def cancel_orders(self, cancel_requests):
        res = api_result()
        res.success = True

        self.orders_cancelled += len(cancel_requests)
        return res

    async def cancel_active_orders(self):
        pass

    async def start(self):
        pass

    async def stop(self):
        pass

    def is_ready(self):
        return self.is_ready_flag

    async def listen(self):
        pass


@pytest.fixture
def cfg_strategy_fixture():
    b = DefaultMunch()
    b.url = "test_url"
    b.api_key = "test_key"
    b.api_secret = "test_secret"

    b.name = "market_maker"
    b.instrument_name = "TEST-PERP"
    b.mid_price_based_calculation = False

    b.tick_size = 1
    b.price_rounding = 2
    b.stop_strategy_on_error = True

    b.positional_retreat = DefaultMunch()
    b.positional_retreat.position_increment = 100
    b.positional_retreat.retreat_ticks = 5

    b.orders = DefaultMunch()
    b.orders.asks = [[0, 1]]
    b.orders.bids = [[0, 1]]
    return b


@pytest.mark.asyncio
async def test_maker_init(cfg_strategy_fixture):
    try:
        strategy = MarketMaker(cfg_strategy_fixture, BittestAdapter())
    except Exception:
        assert False


@pytest.mark.asyncio
async def test_maker_rounding_tob_based(cfg_strategy_fixture):
    try:
        strategy = MarketMaker(cfg_strategy_fixture, BittestAdapter())
    except Exception:
        assert False

    _tob = tob()
    _tob.exchange = "test_exchange"
    _tob.product = "test-perp"
    _tob.best_bid_price = 99.0
    _tob.best_bid_qty = 1
    _tob.best_ask_price = 101.0
    _tob.best_ask_qty = 1
    _tob.timestamp = 0.0

    strategy.tob = _tob

    orders = strategy.generate_orders()

    assert orders[0].price == _tob.best_ask_price
    assert orders[1].price == _tob.best_bid_price


@pytest.mark.asyncio
async def test_maker_rounding_mid_based_1(cfg_strategy_fixture):
    cfg_strategy_fixture.mid_price_based_calculation = True

    try:
        strategy = MarketMaker(cfg_strategy_fixture, BittestAdapter())
    except Exception:
        assert False

    _tob = tob()
    _tob.exchange = "test_exchange"
    _tob.product = "test-perp"
    _tob.best_bid_price = 100.5
    _tob.best_bid_qty = 1
    _tob.best_ask_price = 101.0
    _tob.best_ask_qty = 1
    _tob.timestamp = 0.0

    strategy.tob = _tob

    orders = strategy.generate_orders()

    assert orders[0].price == 101
    assert orders[1].price == 100


@pytest.mark.asyncio
async def test_maker_rounding_mid_based_2(cfg_strategy_fixture):
    cfg_strategy_fixture.mid_price_based_calculation = True

    try:
        strategy = MarketMaker(cfg_strategy_fixture, BittestAdapter())
    except Exception:
        assert False

    _tob = tob()
    _tob.exchange = "test_exchange"
    _tob.product = "test-perp"
    _tob.best_bid_price = 99.0
    _tob.best_bid_qty = 1
    _tob.best_ask_price = 101.0
    _tob.best_ask_qty = 1
    _tob.timestamp = 0.0

    strategy.tob = _tob

    orders = strategy.generate_orders()

    assert orders[0].price == 101
    assert orders[1].price == 99


@pytest.mark.asyncio
async def test_maker_rounding_mid_based_3(cfg_strategy_fixture):
    cfg_strategy_fixture.mid_price_based_calculation = True

    try:
        strategy = MarketMaker(cfg_strategy_fixture, BittestAdapter())
    except Exception:
        assert False

    _tob = tob()
    _tob.exchange = "test_exchange"
    _tob.product = "test-perp"
    _tob.best_bid_price = 95.0
    _tob.best_bid_qty = 1
    _tob.best_ask_price = 105.0
    _tob.best_ask_qty = 1
    _tob.timestamp = 0.0

    strategy.tob = _tob

    orders = strategy.generate_orders()

    assert orders[0].price == 100
    assert orders[1].price == 99


@pytest.mark.asyncio
async def test_maker_positional_retreat(cfg_strategy_fixture):
    try:
        strategy = MarketMaker(cfg_strategy_fixture, BittestAdapter())
    except Exception:
        assert False

    _tob = tob()
    _tob.exchange = "test_exchange"
    _tob.product = "test-perp"
    _tob.best_bid_price = 99.0
    _tob.best_bid_qty = 1
    _tob.best_ask_price = 101.0
    _tob.best_ask_qty = 1
    _tob.timestamp = 0.0

    strategy.tob = _tob

    orders = strategy.generate_orders()

    assert orders[0].price == _tob.best_ask_price
    assert orders[1].price == _tob.best_bid_price

    assert strategy.should_perform_positional_retreat()

    strategy.current_position = 200
    orders = strategy.perform_retreats(orders)

    assert orders[0].price == _tob.best_bid_price - 10*strategy.tick_size
    assert orders[1].price == _tob.best_ask_price
