import json
import pytest
import datetime
from munch import DefaultMunch

from strategy.market_maker import market_maker
from gateways import gateway_interface

from definitions import (
    new_order_ack,
    new_order_nack,
    order_elim_ack,
    order_elim_nack,
    order_fill_ack,
    order_full_fill_ack,
)

class bittest_storage():
    def __init__(self):
        self.uid_to_eid = {}
        self.eid_to_uid = {}

class bittest_adapter(gateway_interface.gateway_interface):

    def __init__(self):
        super().__init__()

        self.is_ready_flag = True
        self.storage = bittest_storage()
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
            self.new_orders_data.append( _request)
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
    b.tick_size = 1
    b.price_rounding = 2
    b.cancel_orders_on_start = False
    b.stop_strategy_on_error = True
    b.cancel_orders_on_reconnection = True

    b.orders = DefaultMunch()
    b.orders.asks = [[1,1]]
    b.orders.bids = [[1,1]]
    return b


@pytest.mark.asyncio
async def test_maker_init(cfg_strategy_fixture):
    try:
        strategy = market_maker(cfg_strategy_fixture, bittest_adapter())
    except Exception:
        assert False

