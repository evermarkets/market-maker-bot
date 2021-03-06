import pytest
from munch import DefaultMunch

from market_maker.orders_manager import OrdersManager
from market_maker.gateways import gateway_interface

from market_maker.definitions import (
    ApiResult,
    OrderRequest,
    OrderType,
    OrderSide,
)


class bittest_storage():
    def __init__(self):
        self.uid_to_eid = {}
        self.eid_to_uid = {}


class bittest_adapter(gateway_interface.GatewayInterface):

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
        res = ApiResult()
        res.success = True

        self.orders_sent += 1
        self.new_orders_data.append(order_request)
        return res

    async def send_orders(self, orders_request):
        res = ApiResult()
        res.success = True

        self.orders_sent += len(orders_request)
        for _request in orders_request:
            self.new_orders_data.append(_request)
        return res

    async def amend_orders(self, new, old):
        res = ApiResult()
        res.success = True

        self.orders_amended += len(new)
        return res

    async def amend_order(self, i, j):
        res = ApiResult()
        res.success = True

        self.orders_amended += 1
        self.amend_orders_data.append((i, j))
        return res

    async def cancel_order(self, cancel_request):
        res = ApiResult()
        res.success = True

        self.orders_cancelled += 1
        return res

    async def cancel_orders(self, cancel_requests):
        res = ApiResult()
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


@pytest.mark.asyncio
async def test_place_orders_1():
    adapter = bittest_adapter()

    om = OrdersManager(adapter)

    buy_order = OrderRequest()
    buy_order.side = OrderSide.buy
    buy_order.type = OrderType.limit
    buy_order.price = 100.0
    buy_order.quantity = 1.0

    sell_order = OrderRequest()
    sell_order.side = OrderSide.sell
    sell_order.type = OrderType.limit
    sell_order.price = 100.0
    sell_order.quantity = 1.0

    await om.place_orders([buy_order, sell_order])

    assert om.exchange_adapter.orders_sent == 2
    assert len(om.orders.values()) == 2
